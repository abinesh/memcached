
typedef struct tagPoint{
    float x;
    float y;
}Point;


typedef struct tagZoneBoundary {
    Point from;
    Point to;
} ZoneBoundary;
ZoneBoundary world_boundary;

typedef struct tag_node_info{
     ZoneBoundary boundary;
    char join_request[10];

}node_info;
node_info nodes[10];


