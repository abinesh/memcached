#include<stdio.h>
#include<string.h>
#include <stdlib.h>

typedef struct tagList{
    int size;
    char **array;
}my_list;


static void mylist_print(my_list *list){
    int i=0;
    fprintf(stderr,"(%d,[",list->size);

    for(i=0;i<list->size;i++){
        fprintf(stderr,"%s,",list->array[i]);
    }
    fprintf(stderr,"])\n");
}

static void mylist_init(my_list *list){
    list->size = 0;
}

static void mylist_add(my_list *list, char* v){
    int new_size = list->size + 1;
    char **new_array = (char**)malloc(sizeof(char*)*new_size);
    int i=0;
    for(i=0;i<new_size;i++){
        if(i == new_size - 1){
            new_array[i]= (char*)malloc(sizeof(char*)*strlen(v));
            sprintf(new_array[i],"%s",v);
        }else{
            new_array[i]= (char*)malloc(sizeof(char*)*strlen(list->array[i]));
            sprintf(new_array[i],"%s",list->array[i]);
        }
    }
    list->size++;
    list->array = new_array;
}

static void mylist_delete_all(my_list *list){
    list->size = 0;
    list->array = NULL;
}

static int mylist_contains(my_list *list, char *v){
    int i=0;
    for(i=0;i<list->size;i++){
        if(strcmp(list->array[i],v) == 0) {
            return 1;
        }
    }
    return 0;
}

static void mylist_delete(my_list *list, char* v){
    if(mylist_contains(list,v) == 1){
        int new_size = list->size - 1;
        char **new_array = (char**)malloc(sizeof(char*)*new_size);
        int i=0;
        int detected = 0;
        for(i=0;i<list->size;i++){
            if(strcmp(list->array[i],v) == 0) {
                detected = 1;
                continue;
            }
            int index = i - detected;
            new_array[index]= (char*)malloc(sizeof(char*)*strlen(list->array[i]));
            sprintf(new_array[index],"%s",list->array[i]);
        }
        list->size--;
        list->array = new_array;
    }
}

int main (int argc, char **argv) {
    my_list *list = (my_list *)malloc(sizeof(my_list));
    mylist_init(list);
    mylist_add(list,"abc1");
    mylist_print(list);
    mylist_add(list,"abc2");
    mylist_print(list);
    mylist_add(list,"abc3");
    mylist_print(list);
    mylist_add(list,"abc4");
    mylist_print(list);
    mylist_delete(list,"abc2");
    mylist_print(list);
    mylist_delete(list,"abc23");
    mylist_print(list);
    printf("%d",mylist_contains(list,"abc1"));
    printf("%d",mylist_contains(list,"abc"));
    mylist_delete_all(list);
    mylist_print(list);

    return 0;

}