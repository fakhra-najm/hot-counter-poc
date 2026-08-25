#include "incrnchk.h"
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct { counter_t *counter; uint64_t delta; uint64_t accepts; } task_t;
static void *race(void *p) { task_t *t=p; for(int i=0;i<100000;i++) if(incrnchk(t->counter,t->delta,0)==INCRNCHK_ACCEPTED)t->accepts++; return 0; }
int main(void) { counter_t c; counter_init(&c,100); uint64_t n;assert(incrnchk(&c,95,&n)==INCRNCHK_ACCEPTED&&n==95);assert(incrnchk(&c,6,&n)==INCRNCHK_REJECTED&&n==95);assert(incrnchk(&c,5,&n)==INCRNCHK_ACCEPTED&&n==100);assert(incrnchk(&c,0,&n)==INCRNCHK_ACCEPTED); counter_init(&c,100000);pthread_t th[8];task_t ts[8];for(int i=0;i<8;i++){ts[i]=(task_t){.counter=&c,.delta=3};pthread_create(&th[i],0,race,&ts[i]);}for(int i=0;i<8;i++)pthread_join(th[i],0);assert(counter_value(&c)<=100000);assert(counter_value(&c)%3==0);puts("PASS strict variable-delta and concurrent CAS");}
