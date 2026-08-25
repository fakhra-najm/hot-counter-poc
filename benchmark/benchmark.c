#include <arpa/inet.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

typedef struct { uint64_t *data; size_t count, capacity; } samples_t;
typedef struct {
    const char *host; int port; unsigned seconds; uint64_t delta; atomic_int *go;
    atomic_int *stop; atomic_ullong *sent, *received, *accepted, *rejected; samples_t samples;
} worker_t;

static uint64_t now_ns(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec * 1000000000ULL + t.tv_nsec; }
static int connect_to(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); struct sockaddr_in a = {.sin_family=AF_INET,.sin_port=htons((uint16_t)port)};
    if (fd < 0 || inet_pton(AF_INET, host, &a.sin_addr) != 1 || connect(fd, (struct sockaddr *)&a, sizeof(a))) { if(fd>=0) close(fd); return -1; } return fd;
}
static void add_sample(samples_t *s, uint64_t v) { if (s->count == s->capacity) { size_t n=s->capacity ? s->capacity*2 : 8192; uint64_t *p=realloc(s->data,n*sizeof(*p)); if(!p) return; s->data=p;s->capacity=n;} s->data[s->count++]=v; }
static void *run_worker(void *opaque) {
    worker_t *w=opaque; int fd=connect_to(w->host,w->port); if(fd<0){perror("connect"); return NULL;} char out[40], in[2]; int l=snprintf(out,sizeof(out),"%llu\n",(unsigned long long)w->delta);
    while(!atomic_load(w->go)) {};
    while(!atomic_load(w->stop)) { uint64_t t=now_ns(); if(write(fd,out,(size_t)l)!=l) break; atomic_fetch_add(w->sent,1); ssize_t got=read(fd,in,2); if(got!=2)break; add_sample(&w->samples,now_ns()-t); atomic_fetch_add(w->received,1); atomic_fetch_add(in[0]=='A'?w->accepted:w->rejected,1); }
    close(fd); return NULL;
}
static int compare(const void *a,const void *b){uint64_t x=*(const uint64_t*)a,y=*(const uint64_t*)b;return (x>y)-(x<y);}
static uint64_t percentile(samples_t *s,double p){if(!s->count)return 0;qsort(s->data,s->count,sizeof(*s->data),compare);size_t i=(size_t)ceil(p*s->count)-1;return s->data[i];}
int main(int argc,char **argv) {
    if(argc<6){fprintf(stderr,"Usage: %s host port clients seconds delta [csv]\n",argv[0]);return 2;} int port=atoi(argv[2]), clients=atoi(argv[3]); unsigned seconds=(unsigned)atoi(argv[4]); uint64_t delta=strtoull(argv[5],NULL,10); if(clients<1||seconds<1){return 2;}
    pthread_t *threads=calloc((size_t)clients,sizeof(*threads)); worker_t *workers=calloc((size_t)clients,sizeof(*workers)); atomic_int go=0,stop=0; atomic_ullong sent=0,received=0,accepted=0,rejected=0;
    for(int i=0;i<clients;i++){workers[i]=(worker_t){.host=argv[1],.port=port,.seconds=seconds,.delta=delta,.go=&go,.stop=&stop,.sent=&sent,.received=&received,.accepted=&accepted,.rejected=&rejected};pthread_create(&threads[i],NULL,run_worker,&workers[i]);}
    struct rusage before,after; getrusage(RUSAGE_SELF,&before); uint64_t started=now_ns(); atomic_store(&go,1); struct timespec pause={.tv_sec=seconds,.tv_nsec=0};nanosleep(&pause,NULL);atomic_store(&stop,1);for(int i=0;i<clients;i++)pthread_join(threads[i],NULL);uint64_t elapsed=now_ns()-started;getrusage(RUSAGE_SELF,&after);
    samples_t all={0};for(int i=0;i<clients;i++){for(size_t j=0;j<workers[i].samples.count;j++)add_sample(&all,workers[i].samples.data[j]);free(workers[i].samples.data);} double elapsed_s=elapsed/1e9;double cpu=((after.ru_utime.tv_sec-before.ru_utime.tv_sec)+(after.ru_utime.tv_usec-before.ru_utime.tv_usec)/1e6+(after.ru_stime.tv_sec-before.ru_stime.tv_sec)+(after.ru_stime.tv_usec-before.ru_stime.tv_usec)/1e6)/elapsed_s*100.;
    uint64_t r=atomic_load(&received); printf("clients=%d duration_s=%.3f tps=%.0f accepted=%llu rejected=%llu p50_us=%.1f p95_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f client_cpu_pct=%.1f samples=%zu\n",clients,elapsed_s,r/elapsed_s,(unsigned long long)atomic_load(&accepted),(unsigned long long)atomic_load(&rejected),percentile(&all,.50)/1e3,percentile(&all,.95)/1e3,percentile(&all,.99)/1e3,percentile(&all,.999)/1e3,percentile(&all,1)/1e3,cpu,all.count);
    if(argc>6){FILE *f=fopen(argv[6],"a");if(!f){perror("csv");return 1;}if(ftell(f)==0)fprintf(f,"clients,duration_s,tps,accepted,rejected,p50_us,p95_us,p99_us,p999_us,max_us,client_cpu_pct,samples\n");fprintf(f,"%d,%.6f,%.2f,%llu,%llu,%.3f,%.3f,%.3f,%.3f,%.3f,%.2f,%zu\n",clients,elapsed_s,r/elapsed_s,(unsigned long long)atomic_load(&accepted),(unsigned long long)atomic_load(&rejected),percentile(&all,.5)/1e3,percentile(&all,.95)/1e3,percentile(&all,.99)/1e3,percentile(&all,.999)/1e3,percentile(&all,1)/1e3,cpu,all.count);fclose(f);} free(all.data);free(threads);free(workers);
}
