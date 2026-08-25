#ifndef ROUTER_H
#define ROUTER_H
#include <stdatomic.h>
typedef enum { ROUTE_NORMAL = 0, ROUTE_PEAK = 1, ROUTE_DANGER = 2 } route_mode_t;
typedef struct { _Atomic int mode; } router_t;
void router_init(router_t *router);
void router_set_mode(router_t *router, route_mode_t mode);
route_mode_t router_get_mode(const router_t *router);
#endif
