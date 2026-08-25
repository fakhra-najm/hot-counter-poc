#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

typedef enum { SHARED_STRICT, RESERVED_STRICT } lane_mode_t;
typedef struct { _Atomic uint64_t value; uint64_t limit; } component_t;
typedef struct { component_t *components; int lane, lanes; lane_mode_t mode; uint64_t delta, until; _Atomic int *go; _Atomic uint64_t *ops, *accepts; } worker_t;
static uint64_t now_ns(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec; }
static int update(component_t *c, uint64_t d) { uint64_t old=atomic_load_explicit(&c->value,memory_order_relaxed); do { if(d>c->limit-old)return 0; } while(!atomic_compare_exchange_weak_explicit(&c->value,&old,old+d,memory_order_seq_cst,memory_order_relaxed)); return 1; }
static void *run(void *opaque) { worker_t*w=opaque; while(!atomic_load(w->go)){} component_t*c=w->mode==SHARED_STRICT?&w->components[0]:&w->components[w->lane]; while(now_ns()<w->until){atomic_fetch_add(w->ops,1);if(update(c,w->delta))atomic_fetch_add(w->accepts,1);}return 0; }
int main(int argc,char**argv){if(argc!=5){fprintf(stderr,"Usage: %s shared|reserved lanes seconds delta\\n",argv[0]);return 2;}lane_mode_t m=argv[1][0]=='s'?SHARED_STRICT:RESERVED_STRICT;int n=atoi(argv[2]),secs=atoi(argv[3]);uint64_t d=strtoull(argv[4],0,10);if(n<1||secs<1||d<1)return 2;component_t*c=calloc((size_t)n,sizeof(*c));pthread_t*t=calloc((size_t)n,sizeof(*t));worker_t*w=calloc((size_t)n,sizeof(*w));uint64_t limit=UINT64_MAX/4;for(int i=0;i<n;i++){c[i].limit=m==SHARED_STRICT?limit:limit/(uint64_t)n;} _Atomic int go=0;_Atomic uint64_t ops=0,accepts=0;uint64_t deadline=now_ns()+(uint64_t)secs*1000000000ULL;for(int i=0;i<n;i++){w[i]=(worker_t){.components=c,.lane=i,.lanes=n,.mode=m,.delta=d,.until=deadline,.go=&go,.ops=&ops,.accepts=&accepts};pthread_create(&t[i],0,run,&w[i]);}atomic_store(&go,1);for(int i=0;i<n;i++)pthread_join(t[i],0);uint64_t total=0;for(int i=0;i<n;i++)total+=atomic_load(&c[i].value);printf("mode=%s lanes=%d ops=%llu accepts=%llu tps=%.0f logical_total=%llu\\n",m==SHARED_STRICT?"shared_strict":"reserved_strict",n,(unsigned long long)ops,(unsigned long long)accepts,atomic_load(&accepts)/(double)secs,(unsigned long long)total);free(w);free(t);free(c);}
