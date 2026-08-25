#include "connection.h"
#include "hotkey_detector.h"
#include "router.h"
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void usage(const char *p) { fprintf(stderr, "Usage: %s [port] [limit] [hot_tps=0]\n", p); }
int main(int argc, char **argv) {
    const int port = argc > 1 ? atoi(argv[1]) : 9090;
    const uint64_t limit = argc > 2 ? strtoull(argv[2], NULL, 10) : UINT64_MAX;
    const uint64_t hot_tps = argc > 3 ? strtoull(argv[3], NULL, 10) : 0;
    if (port < 1 || port > 65535) { usage(argv[0]); return 2; }
    signal(SIGPIPE, SIG_IGN);
    int fd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    if (fd < 0 || setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes))) { perror("socket"); return 1; }
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=htons((uint16_t)port), .sin_addr.s_addr=htonl(INADDR_ANY)};
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr))) { perror("bind"); return 1; }
    if (listen(fd, 1024)) { perror("listen"); return 1; }
    counter_store_t store; server_metrics_t metrics = {0}; counter_store_init(&store, limit);
    router_t router; router_init(&router); hotkey_detector_t detector;
    if (hot_tps && hotkey_detector_start(&detector, &metrics, &router, hot_tps, 100) != 0) { perror("hotkey_detector"); return 1; }
    printf("counter-server listening port=%d limit=%llu\n", port, (unsigned long long)limit); fflush(stdout);
    for (;;) {
        int client = accept(fd, NULL, NULL); if (client < 0) continue;
        connection_args_t *a = malloc(sizeof(*a)); if (!a) { close(client); continue; }
        *a = (connection_args_t){.fd=client, .store=&store, .metrics=&metrics}; pthread_t thread;
        if (pthread_create(&thread, NULL, connection_run, a) == 0) pthread_detach(thread); else { close(client); free(a); }
    }
}
