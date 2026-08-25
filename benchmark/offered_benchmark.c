#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

typedef struct { uint64_t *values; size_t count, capacity; } samples_t;
typedef struct { const char* host; int port, worker, workers; uint64_t target, delta, deadline; atomic_int* go; atomic_ullong* completed; samples_t samples; } worker_t;
static uint64_t clock_ns(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec; }
static int connect_to(const char* host,int port) { int fd=socket(AF_INET,SOCK_STREAM,0); struct sockaddr_in a={.sin_family=AF_INET,.sin_port=htons((uint16_t)port)}; if(fd<0||inet_pton(AF_INET,host,&a.sin_addr)!=1||connect(fd,(struct sockaddr*)&a,sizeof(a))){if(fd>=0)close(fd);return -1;}return fd; }
static void add(samples_t*s,uint64_t v){if(s->count==s->capacity){size_t c=s->capacity?s->capacity*2:4096;uint64_t*p=realloc(s->values,c*sizeof(*p));if(!p)return;s->values=p;s->capacity=c;}s->values[s->count++]=v;}
static void* run(void* opaque) { worker_t*w=opaque;int fd=connect_to(w->host,w->port);if(fd<0){perror("connect");return NULL;}char out[40],in[2];int n=snprintf(out,sizeof(out),"%llu\n",(unsigned long long)w->delta);while(!atomic_load(w->go)){}const uint64_t gap=1000000000ULL*(uint64_t)w->workers/w->target;uint64_t next=clock_ns()+gap*(uint64_t)w->worker;while(clock_ns()<w->deadline){for(;;){uint64_t now=clock_ns();if(now>=next)break;uint64_t wait=next-now;struct timespec pause={.tv_sec=(time_t)(wait/1000000000ULL),.tv_nsec=(long)(wait%1000000000ULL)};nanosleep(&pause,NULL);}next+=gap;uint64_t start=clock_ns();if(write(fd,out,(size_t)n)!=n||read(fd,in,2)!=2)break;add(&w->samples,clock_ns()-start);atomic_fetch_add(w->completed,1);}close(fd);return NULL;}
static int cmp(const void*a,const void*b){uint64_t x=*(const uint64_t*)a,y=*(const uint64_t*)b;return(x>y)-(x<y);}static uint64_t pct(samples_t*s,double p){if(!s->count)return 0;qsort(s->values,s->count,sizeof(*s->values),cmp);return s->values[(size_t)ceil(p*s->count)-1];}
int main(int argc,char**argv){if(argc!=7){fprintf(stderr,"Usage: %s host port clients seconds target_tps delta\n",argv[0]);return 2;}int clients=atoi(argv[3]),seconds=atoi(argv[4]);uint64_t target=strtoull(argv[5],0,10),delta=strtoull(argv[6],0,10);if(clients<1||seconds<1||target<(uint64_t)clients)return 2;pthread_t*threads=calloc((size_t)clients,sizeof(*threads));worker_t*workers=calloc((size_t)clients,sizeof(*workers));atomic_int go=0;atomic_ullong completed=0;uint64_t deadline=clock_ns()+(uint64_t)seconds*1000000000ULL;for(int i=0;i<clients;i++){workers[i]=(worker_t){.host=argv[1],.port=atoi(argv[2]),.worker=i,.workers=clients,.target=target,.delta=delta,.deadline=deadline,.go=&go,.completed=&completed};pthread_create(&threads[i],0,run,&workers[i]);}uint64_t start=clock_ns();atomic_store(&go,1);samples_t all={0};for(int i=0;i<clients;i++){pthread_join(threads[i],0);for(size_t j=0;j<workers[i].samples.count;j++)add(&all,workers[i].samples.values[j]);free(workers[i].samples.values);}double elapsed=(clock_ns()-start)/1e9;uint64_t done=atomic_load(&completed);printf("target_tps=%llu actual_tps=%.0f clients=%d p50_us=%.1f p95_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f samples=%zu\n",(unsigned long long)target,done/elapsed,clients,pct(&all,.5)/1e3,pct(&all,.95)/1e3,pct(&all,.99)/1e3,pct(&all,.999)/1e3,pct(&all,1)/1e3,all.count);free(all.values);free(workers);free(threads);}
