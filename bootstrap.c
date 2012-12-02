#include "bootstrap.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <pthread.h>
#include <unistd.h>

#include <signal.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/wait.h>

#define NODE_ADDITION_PORT "11311"
#define METADATA_UPDATE_PORT "11312"
#define NODE_DEPARTURE_PORT "11313"
#define BACKLOG 10 // how many pending connections queue will hold


void init_boundary(ZoneBoundary *b){
    b->from.x = 0;
    b->from.y = 0;
    b->to.x = 0;
    b->to.y = 0;
}


/// Start of functions common to kmain.c

static void serialize_boundary(ZoneBoundary b, char *s) {
	sprintf(s, "[(%f,%f) to (%f,%f)]", b.from.x, b.from.y, b.to.x, b.to.y);
}

static void deserialize_boundary(char *s, ZoneBoundary *b) {
	sscanf(s, "[(%f,%f) to (%f,%f)]", &(b->from.x), &(b->from.y), &(b->to.x),
			&(b->to.y));
}
static void sigchld_handler(int s) {
	while (waitpid(-1, NULL, WNOHANG) > 0);
}

// get sockaddr, IPv4 or IPv6:
static void *get_in_addr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*) sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*) sa)->sin6_addr);
}

static int receive_connection_from_client(int server_sock_fd, char *caller){
    int new_fd; // listen on sock_fd, new connection on new_fd
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    char s[INET6_ADDRSTRLEN];

    fprintf(stderr,"%s : server: waiting for connections...\n",caller);
    sin_size = sizeof their_addr;
    new_fd = accept(server_sock_fd, (struct sockaddr *) &their_addr, &sin_size);
    if (new_fd == -1) {
        perror("accept");
        exit(-1);
    }

    inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *) &their_addr), s, sizeof s);
    fprintf(stderr, "%s : server: got connection from %s\n",caller, s);
    return new_fd;
}

static int listen_on(char *port,char *caller){
    int sockfd=-1; // listen on sock_fd, new connection on new_fd
	struct sigaction sa;
   	struct addrinfo hints, *servinfo, *p;
   	int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = INADDR_ANY;

	if ((rv = getaddrinfo("localhost", port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "In %s, getaddrinfo: %s\n", caller, gai_strerror(rv));
        exit(-1);
    }

    // loop through all the results and bind to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            fprintf(stderr,"In %s,",caller);
            perror("listener: socket");
            continue;
        }
        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            fprintf(stderr,"In %s,",caller);
            perror("listener: bind");
            continue;
        }
        break;
    }
    if (p == NULL) {
        fprintf(stderr, "In %s, listener: failed to bind socket\n",caller);
        exit(-1);
    }

    if (listen(sockfd, BACKLOG) == -1) {
    fprintf(stderr,"In %s,",caller);
    perror("listen");
    exit(1);
    }

    sa.sa_handler = sigchld_handler; // reap all dead processes
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    if (sigaction(SIGCHLD, &sa, NULL ) == -1) {
    fprintf(stderr,"In %s,",caller);
    perror("sigaction");
    exit(1);
    }

    return sockfd;
}

static ZoneBoundary* _recv_boundary_from_neighbour(int child_fd) {
	char buf[1024];
	int MAXDATASIZE = 1024;
	memset(buf, '\0', 1024);
	if (recv(child_fd, buf, MAXDATASIZE-1, 0) == -1) {
        perror("recv");
        exit(1);
    }
	ZoneBoundary *child_boundary = (ZoneBoundary *) malloc(sizeof(ZoneBoundary));
	deserialize_boundary(buf, child_boundary);
	fprintf(stderr,"Received %s\n",buf);
	return child_boundary;
}


static float calculate_area(ZoneBoundary bounds)
{
	Point from,to;
	from.x=bounds.from.x;
	from.y=bounds.from.y;
	to.x=bounds.to.x;
	to.y=bounds.to.y;
	float area;
	fprintf(stderr,"\npoints::%f,%f,%f,%f\n",to.x,from.x,to.y,from.y);
	area= (to.x - from.x)* (to.y-from.y);
	fprintf(stderr,"%f",(to.x - from.x)*(to.y-from.y));
	return area;
}

/// End of functions common to kmain.c

static int find_port(){
	struct addrinfo hints, *servinfo, *p;
		int rv,addrlen;
		int socket_descriptor=0,portno;
		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_INET;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = INADDR_ANY;
		struct sockaddr_in serv_addr;
	int input_portno = 0;
	char portnoString[10];
		sprintf(portnoString,"%i", input_portno);

		if ((rv = getaddrinfo(NULL, portnoString, &hints, &servinfo)) != 0) {
				fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
				//return 1;
		}

		// loop through all the results and bind to the first we can
		for(p = servinfo; p != NULL; p = p->ai_next) {
			if ((socket_descriptor = socket(p->ai_family, p->ai_socktype,
					p->ai_protocol)) == -1) {
					perror("listener: socket");
					continue;
			}
			if (bind(socket_descriptor, p->ai_addr, p->ai_addrlen) == -1) {
				close(socket_descriptor);
				perror("listener: bind");
				continue;
			}
			break;
		}
		if (p == NULL) {
			fprintf(stderr, "listener: failed to bind socket\n");
			//return 2;
		}
		addrlen = sizeof(serv_addr);
		int getsock_check=getsockname(socket_descriptor,(struct sockaddr *)&serv_addr, (socklen_t *)&addrlen) ;

		   	if (getsock_check== -1) {
		   			perror("getsockname");
		   			exit(1);
		   	}
		portno =  ntohs(serv_addr.sin_port);
        	fprintf(stderr, "The actual port number is %d\n", portno);
		freeaddrinfo(servinfo);
		close(socket_descriptor);
		return portno;

}

static int cluster_has_nodes(){
    int counter;
    for(counter=0;counter<10;counter++)
    {
        if(strcmp(nodes[0].join_request,"NULL")!=0)
        {
            //found a node in the list
            return 1;
        }
    }
    // Did not find any node in the list
    return -1;
}

static int find_node_to_join(){

	int port;
	int counter;
	float max=-99999;
	float area;
	int final_counter;

	for(counter=0;counter<10;counter++)
	{
		area = calculate_area(nodes[counter].boundary);
		if(max<area)
		{
			max = area;
			final_counter=counter;
		}
	}
	port=atoi(nodes[final_counter].join_request);
	return port;

}

static void save_port_number(int port){
	int counter;

	for(counter=0;counter<10;counter++)
	{
		if(!strcmp(nodes[counter].join_request,"NULL"))
		{
			sprintf(nodes[counter].join_request,"%d",port);
			break;
		}
	}
}


static void print_list_of_nodes_in_cluster(){
	int counter;
    fprintf(stderr,"List of nodes in the cluster:\n");
	for(counter=0;counter<10;counter++)
	{
		if(strcmp(nodes[counter].join_request,"NULL"))
		{
		    fprintf(stderr,"\t%d: (%s,[(%f,%f) to (%f,%f)])\n",
		            (counter+1),nodes[counter].join_request,
		            nodes[counter].boundary.from.x,
		            nodes[counter].boundary.from.y,
		            nodes[counter].boundary.to.x,
		            nodes[counter].boundary.to.y);
		}
	}
	fprintf(stderr,"End of list\n");
}

static void *node_addition_routine(void *arg){
    fprintf(stderr,"Node addition thread started\n");
    int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
    int port,port_to_join;
    char portnum[255];
    char str[1024];

    sockfd  = listen_on(NODE_ADDITION_PORT,"node_addition_routine");
    printf("node_addition_routine, port %s: waiting for connections...\n",NODE_ADDITION_PORT);
    
    while(1) { // main accept() loop
        new_fd = receive_connection_from_client(sockfd,"node_addition_routine");

        //sending join_req_port
        port=find_port();
        sprintf(portnum,"%d",port);
        if (send(new_fd,portnum,strlen(portnum), 0) == -1)
        perror("send");
        
        serialize_boundary(world_boundary,str);
        
        usleep(1000);
        //sending world boundary
        if (send(new_fd,str,strlen(str), 0) == -1)
        perror("send");
        
        usleep(1000);
        
        //sending whom to connect
        if(cluster_has_nodes() == -1)
        {
            nodes[0].boundary=world_boundary;
            sprintf(nodes[0].join_request,"%d",port);
            sprintf(portnum,"%s %d","FIRST",0);
            send(new_fd,portnum,strlen(portnum), 0);
        }
        else{
            port_to_join=find_node_to_join();
            save_port_number(port);
            sprintf(portnum,"%s %d","NOTFIRST",port_to_join);
            send(new_fd,portnum,strlen(portnum), 0);
        }
        close(new_fd); // parent doesn't need this
        print_list_of_nodes_in_cluster();
    }
    return 0;
}

static void save_boundaries(char *port_number,ZoneBoundary b){
	int counter;
	for(counter=0;counter<10;counter++)
	{
		if(!strcmp(nodes[counter].join_request,port_number))
		{
			nodes[counter].boundary.from.x=b.from.x;
			nodes[counter].boundary.from.y=b.from.y;
			nodes[counter].boundary.to.x=b.to.x;
			nodes[counter].boundary.to.y=b.to.y;
			break;
		}
	}
}

static void remove_node(char *port_number,ZoneBoundary b){
	int counter;
	for(counter=0;counter<10;counter++)
	{
		if(!strcmp(nodes[counter].join_request,port_number))
		{
			strcpy(nodes[counter].join_request,"NULL");
			init_boundary(&nodes[counter].boundary);
		}
	}
}

static void *metadata_update_routine(void *arg){
    fprintf(stderr,"metadata_update_routine started\n");
    int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
    int numbytes;
    char buf[1024],port_number[255],parent_port_number[255];
    ZoneBoundary my_boundary,parent_boundary;

    sockfd = listen_on(METADATA_UPDATE_PORT,"metadata_update_routine");
	printf("metadata_update_routine, port %s: waiting for connections...\n", METADATA_UPDATE_PORT);

    while(1) { // main accept() loop
        new_fd = receive_connection_from_client(sockfd,"metadata_update_routine");

        //receiving child boundary
        my_boundary = *(_recv_boundary_from_neighbour(new_fd));
        
        //receiving child port number
        memset(buf, '\0', 1024);
        if ((numbytes = recv(new_fd, buf,1024, 0)) == -1) {
            perror("rec");
            exit(1);
        }
        
        
        sscanf(buf,"%s",port_number);
        save_boundaries(port_number,my_boundary);
        
        //receiving parent boundary
        parent_boundary = *(_recv_boundary_from_neighbour(new_fd));

        
        //receiving parent port
        memset(buf, '\0', 1024);
        if ((numbytes = recv(new_fd, buf,1024, 0)) == -1) {
            perror("rec");
            exit(1);
        }
        
        sscanf(buf,"%s",parent_port_number);
        save_boundaries(parent_port_number,parent_boundary);
        
        close(new_fd); // parent doesn't need this
        print_list_of_nodes_in_cluster();
    }
}

static void *node_depature_routine(void *arg){
    fprintf(stderr,"node_depature_routine started\n");
    int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
    int numbytes;
    char buf[1024],port_number[255],parent_port_number[255];
    ZoneBoundary my_boundary,parent_boundary;
    sockfd = listen_on(NODE_DEPARTURE_PORT,"node_depature_routine");

    printf("node_depature_routine, port %s: waiting for connections...\n",NODE_DEPARTURE_PORT);
    
    while(1) { // main accept() loop
        new_fd = receive_connection_from_client(sockfd,"node_depature_routin");

        //receiving child boundary
        my_boundary = *(_recv_boundary_from_neighbour(new_fd));

        
        //receiving child port number
        memset(buf, '\0', 1024);
        if ((numbytes = recv(new_fd, buf,1024, 0)) == -1) {
            perror("rec");
            exit(1);
        }
        
        fprintf(stderr,"\nchild portnum recv:%s\n",buf);
        sscanf(buf,"%s",port_number);
        remove_node(port_number,my_boundary);
        
        //receiving parent boundary
        parent_boundary = *(_recv_boundary_from_neighbour(new_fd));

        //receiving parent port
        memset(buf, '\0', 1024);
        if ((numbytes = recv(new_fd, buf,1024, 0)) == -1) {
            perror("rec");
            exit(1);
        }
        
        fprintf(stderr,"\nchild recv:%s\n",buf);
        sscanf(buf,"%s",parent_port_number);
        
        save_boundaries(parent_port_number,parent_boundary);
        
        close(new_fd); // parent doesn't need this
        print_list_of_nodes_in_cluster();
    }
}

int main(void){
	int i;
	printf("Bootstrap running\n");
	world_boundary.from.x=0;
	world_boundary.from.y=0;
	world_boundary.to.x=50;
	world_boundary.to.y=50;
	
	for(i =0 ;i < 10 ;i++)
    {
			strcpy(nodes[i].join_request,"NULL");
			init_boundary(&nodes[i].boundary);
    }
    print_list_of_nodes_in_cluster();

    pthread_t node_addition_thread;
    pthread_t metadata_update_thread;
    pthread_t node_departure_thread;
    
    pthread_create(&node_addition_thread, 0,node_addition_routine,NULL);
    pthread_create(&metadata_update_thread, 0,metadata_update_routine,NULL);
    pthread_create(&node_departure_thread, 0,node_depature_routine,NULL);
    pthread_join(node_addition_thread,NULL);
    pthread_join(metadata_update_thread,NULL);
    pthread_join(node_departure_thread,NULL);
    return 0;
}

