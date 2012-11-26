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



#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/wait.h>

#define PORT "11311" // the port users will be connecting to
#define BACKLOG 10 // how many pending connections queue will hold



static void serialize_boundary(ZoneBoundary b, char *s) {
	sprintf(s, "[(%f,%f) to (%f,%f)]", b.from.x, b.from.y, b.to.x, b.to.y);
}


void sigchld_handler(int s)
{
	while(waitpid(-1, NULL, WNOHANG) > 0);
}
// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
	return &(((struct sockaddr_in*)sa)->sin_addr);
	}
return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


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

static int check(){
int counter;
for(counter=0;counter<10;counter++)
		{
			if(!strcmp(nodes[0].join_request,"NULL"))
			{
				printf("This is first node");
				return 1;
			}
			else
				return -1;
		}

return -1;

}



static float calculate_area(ZoneBoundary bounds)
{
	Point from,to;
	from.x=bounds.from.x;
	from.y=bounds.from.y;
	to.x=bounds.to.x;
	to.y=bounds.to.y;
	float area;

	area= (to.x - from.x)* (to.y-from.y);
	fprintf(stderr,"%f",(to.x - from.x)*(to.y-from.y));
	return area;
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






static int start_listening(){
int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
struct addrinfo hints, *servinfo, *p;
struct sockaddr_storage their_addr; // connector's address information
socklen_t sin_size;
struct sigaction sa;
int yes=1;
char s[INET6_ADDRSTRLEN];
int rv;
memset(&hints, 0, sizeof hints);
hints.ai_family = AF_UNSPEC;
hints.ai_socktype = SOCK_STREAM;
hints.ai_flags = AI_PASSIVE; // use my IP
int port;
char portnum[255];
char str[1024];
int first_node;

if ((rv = getaddrinfo("localhost", PORT, &hints, &servinfo)) != 0) {
	fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
	return 1;
}
// loop through all the results and bind to the first we can

for(p = servinfo; p != NULL; p = p->ai_next) {
	if ((sockfd = socket(p->ai_family, p->ai_socktype,
		p->ai_protocol)) == -1) {
		perror("server: socket");
		continue;
	}

	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}
	if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
		close(sockfd);
		perror("server: bind");
		continue;
	}
	break;
}

	if (p == NULL) {
		fprintf(stderr, "server: failed to bind\n");
		return 2;
	}

	freeaddrinfo(servinfo); // all done with this structure
	if (listen(sockfd, BACKLOG) == -1) {
	perror("listen");
	exit(1);
}
	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	}
	
	printf("server: waiting for connections...\n");

	while(1) { // main accept() loop
		sin_size = sizeof their_addr;
		new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
		if (new_fd == -1) {
			perror("accept");
			continue;
		}
		inet_ntop(their_addr.ss_family,
		get_in_addr((struct sockaddr *)&their_addr),
		s, sizeof s);

		printf("server: got connection from %s\n", s);


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
		first_node=check();
		printf("\nfirst node is:%d\n",first_node);
		if(first_node==1)
		{

			nodes[0].boundary=world_boundary;
			sprintf(nodes[0].join_request,"%d",port);
			sprintf(portnum,"%s %d","FIRST",0);
			send(new_fd,portnum,strlen(portnum), 0);

		}
		else{

			port=find_node_to_join();
			sprintf(portnum,"%s %d","NOTFIRST",port);
			send(new_fd,portnum,strlen(portnum), 0);
		}








		close(new_fd); // parent doesn't need this
	}



}



int main(void){
	int counter,i;
	printf("hello\n");
	world_boundary.from.x=0;
	world_boundary.from.y=0;
	world_boundary.to.x=5;
	world_boundary.to.y=5;
	
	for(i =0 ;i < 10 ;i++)
		{
			strcpy(nodes[i].join_request,"NULL");
			nodes[i].boundary.from.x=0;
			nodes[i].boundary.from.y=0;
			nodes[i].boundary.to.x=0;
			nodes[i].boundary.to.y=0;
		}


	start_listening();

  return 0;
}

