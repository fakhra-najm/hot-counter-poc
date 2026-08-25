#include "router.h"
void router_init(router_t *router) { atomic_init(&router->mode, ROUTE_NORMAL); }
void router_set_mode(router_t *router, route_mode_t mode) { atomic_store_explicit(&router->mode, mode, memory_order_release); }
route_mode_t router_get_mode(const router_t *router) { return (route_mode_t)atomic_load_explicit(&router->mode, memory_order_acquire); }
