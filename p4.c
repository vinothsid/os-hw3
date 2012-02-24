#include<stdio.h>
#include<string.h>
#include<assert.h>
#include<stdbool.h>
#include "myatomic.h"
#define MAX_MOVIE_LEN 51
#define MAX_COUNTRY_LEN 51
#define MAX_LINE_LEN 100
#define MAX_NUM_COUNTRY 5
#define NUM_THREADS_PER_COUNTRY 5
#define MAX_BUFFER_SIZE 1024*1024*10 // 10 mb
#define HASH_TABLE_SIZE 257

#define DEBUG

struct block {
        int sIndex;
        int eIndex;
} offsets[MAX_NUM_COUNTRY][NUM_THREADS_PER_COUNTRY] ;


struct node {
	char movie[MAX_MOVIE_LEN];
	char country[MAX_COUNTRY_LEN];
	long int numVotes;
	int rating;
	int year;
	struct node *next;
};

typedef struct node Node; 

struct hashNode {
	Node *begin;
	Node *end;
} hashTable[HASH_TABLE_SIZE];


char countries[MAX_NUM_COUNTRY][MAX_COUNTRY_LEN];
char buffer[MAX_NUM_COUNTRY][MAX_BUFFER_SIZE];
int numCountries;

int getCountries(char *fileName) {
        FILE *fp;
        fp = fopen(fileName,"r");
        assert(fp);
        int i=0;
#ifdef DEBUG
	printf("Countries:\n=========================\n");
#endif
        while(i < MAX_NUM_COUNTRY && fscanf(fp,"%[^\n]\n",countries[i++])==1 ) {
#ifdef DEBUG
		printf("%s\n",countries[i-1]);
#endif
	}

        fclose(fp);
        return i-1;
}

int searchCountry(char *c) {
	int i=0;
	for(i=0; i < numCountries;i++) {
		if(strcmp(c,countries[i])==0) {
			return i;
		}

	}

	return -1;
}

int addToBuffer(char *line,char *country) {
	int index = searchCountry(country);
	if(index == -1) {
		printf("ERROR: Country : %s not found in the country list\n",country);
		return -1;
	}
	int len = strlen(line);
	line[ len ] = '\n' ;
	line[ len+1 ] = '\0';
	strcat(buffer[index],line);
	return index;

}

void fillBuffer(char *fileName) {
	char fName[50]={0};
	char *cName;
	char line[MAX_LINE_LEN];
	FILE *fp, *fp1;
        fp = fopen(fileName,"r");
        assert(fp);
        int i=0,len=0;
        while( fscanf(fp,"%[^\n]\n",fName )==1 ) {
		if(fName[strlen(fName)-1] == ' ')
			fName[strlen(fName)-1] = '\0';
		fp1 = fopen(fName,"r");
#ifdef DEBUG
		printf("=======================\nFile Name: %s\n",fName);
#endif
		while( fscanf(fp1,"%[^\n]\n",line) == 1 ) {
#ifdef DEBUG
			printf("%s\n",line);
#endif

			len = strlen(line);
			i=len-1;
			while(i >= 0 && line[i] != ':'  ) {
				i--;
			}
			cName = line+i+1;
#ifdef DEBUG
			printf("---Country : %s----\n", cName );
#endif

			addToBuffer(line,cName );
		}

		fclose(fp1);
	}

        fclose(fp);

}

int findOffsets(char *fileContent, int threadCount,struct block * fBlocks ) {
        int blockSize,i,start=0,end=0,size=0;
        char newLine;

	size = strlen(fileContent);
	
        int tc=threadCount;
        int remChars = size;
        for(i=0;i<threadCount && end < size;i++) {
                blockSize = remChars/tc;
                if( (start+blockSize-1 ) <= size ) {
                        end=end+blockSize;
                }
                do {
                        newLine = fileContent[end++];
                } while(newLine!='\n' && newLine!='\0');

		(fBlocks+i)->sIndex = start;
		(fBlocks+i)->eIndex = end;
		remChars=size-end;

		tc--;
		start=end+1;
        }

        return size;
}
bool removeLowerRatingFromList(int index , char *country,long int numVotes , int rating , int year ) {
        if(hashTable[index].end == NULL){
                return false;
        }

        Node *temp = hashTable[index].begin;
        Node *prev = NULL;

        while(temp != NULL){
                if(strcmp(temp->country,country) == 0 && temp->year == year &&  \
		    ( temp->rating < rating || \
		      (temp->rating == rating && temp->numVotes < numVotes))  ) {
                        if(prev == NULL){
                                hashTable[index].begin = hashTable[index].begin->next;
                                free(temp);
                                return true;
                                }

                        if(temp->next == NULL){
                                hashTable[index].end = prev;
                        }

                        prev->next = temp->next;
                        free(temp);
                        return true;
                }
                prev = temp;
                temp = temp->next;
        }
        return false;
}


bool insert(int index,char *movie,char *country,long int numVotes,int rating,int year) {
        bool success = false;

	bool flag = false;
        Node *new = NULL;
        new = malloc(sizeof(Node));
        if(new == NULL){
                fprintf(stderr,"addToList: Malloc failed");
                return false;
        }

	strcpy(new->movie , movie);
	strcpy(new->country , country);
	new->numVotes = numVotes;
	new->rating = rating;
	new->year = year;
	new->next = NULL;

	


	if ( hashTable[index].begin == NULL || hashTable[index].end == NULL ) {
		hashTable[index].begin = new;
		hashTable[index].end = new;
	} else {

		removeLowerRatingFromList(index,country,numVotes,rating,year);

		if ( hashTable[index].begin == NULL || hashTable[index].end == NULL ) {
			hashTable[index].begin = new;
			hashTable[index].end = new;
		} else {

			hashTable[index].end->next = new;
			hashTable[index].end = hashTable[index].end->next ;
		}
	}
        return true;
}

void print() {
	int i=0;
	Node *p;
	for(i=0;i< HASH_TABLE_SIZE; i++ ) {
		p=hashTable[i].begin;
		while(p != NULL) {
			printf("%d => %s:%s:%ld:%d:%d\n",i,p->movie,p->country,p->numVotes,p->rating,p->year);
			p=p->next;
		}
	}

}

int hashFunc(int year,char *country) {
	
	int hashVal=0,i=0;
	for(i=0; i<strlen(country); i++) {
		hashVal += country[i] * (i+1) ;
	}

	hashVal+=year;
	hashVal = hashVal%HASH_TABLE_SIZE;
	return hashVal;	
}

void *threadFunc() {

}

int main() {


	numCountries = getCountries("countries.txt");
	fillBuffer("p4-in.txt");

	int i=0,j=0;
#ifdef DEBUG
	printf("==========Buffer Contents===========\n");
	for(i=0; i<numCountries; i++) {
		printf("%s",buffer[i]);
	}
#endif

	
	for(i=0;i<numCountries;i++ )	 {
		findOffsets(buffer[i],NUM_THREADS_PER_COUNTRY,offsets[i]);
	}

#ifdef DEBUG
	printf("======== Offsets =========\n");
	for(i=0;i< numCountries;i++) {
		printf("==== Country : %s ====\n" , countries[i]);
		for(j=0;j<NUM_THREADS_PER_COUNTRY ; j++ ) {
			printf("Thread: #%d start: %d end: %d\n",j,offsets[i][j].sIndex , offsets[i][j].eIndex);
		}
	}
#endif

	insert(0,"movie","country",90000,7,2009);
	insert(0,"movie","country",90000,8,2009);
/*	insert(0,"movie1","country1",90000,7,2009);

	insert(0,"movie2","country2",90000,7,2009);
	insert(1,"movie2","country2",90000,7,2009);
*/
	print();

/*
	printf("hash of india ,1965 : %d\n",hashFunc(1965,"india"));
	printf("hash of USA ,1965 : %d\n",hashFunc(1965,"USA"));
	printf("hash of china ,1965 : %d\n",hashFunc(1965,"china"));
	printf("hash of japan ,1965 : %d\n",hashFunc(1965,"japan"));
	printf("hash of napaj ,1965 : %d\n",hashFunc(1965,"napaj"));*/
	
}
