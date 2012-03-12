#include<stdio.h>
#include<string.h>
#include<assert.h>
#include<stdbool.h>
#include<pthread.h>
#include "myatomic.h"
#define MAX_MOVIE_LEN 51
#define MAX_COUNTRY_LEN 51
#define MAX_NUM_YEARS 51
#define MAX_LINE_LEN 100
#define MAX_NUM_COUNTRY 5
#define NUM_THREADS_PER_COUNTRY 5
#define MAX_BUFFER_SIZE 1024*1024*10 // 10 mb
#define HASH_TABLE_SIZE 257
#define START_YEAR 1962

#define print(string) write(1,string,strlen(string))

//#define DEBUG
//#define VERBOSE

pthread_mutex_t debugLock = PTHREAD_MUTEX_INITIALIZER;

struct block {
        int sIndex;
        int eIndex;
} offsets[MAX_NUM_COUNTRY][NUM_THREADS_PER_COUNTRY] ;


struct movieInfo {
        long int numVotes;
        int rating;
        char movie[MAX_MOVIE_LEN];
};

struct node {
	int year;
	char country[MAX_COUNTRY_LEN];
	struct movieInfo *m;
	struct node *next;
};

typedef struct node Node; 


/*
struct hashNode {
	Node *begin;
	Node *end;
} hashTable[HASH_TABLE_SIZE];
*/

struct threadInfo {
	int countryIndex;
	int threadId;
};

typedef struct threadInfo ThreadInfo;

Node *hashTable[HASH_TABLE_SIZE];
int locks[HASH_TABLE_SIZE];
int count[MAX_NUM_COUNTRY][MAX_NUM_YEARS];
int totalCount[MAX_NUM_YEARS];
char countries[MAX_NUM_COUNTRY][MAX_COUNTRY_LEN];
char buffer[MAX_NUM_COUNTRY][MAX_BUFFER_SIZE];
int numCountries;

void init_locks() {
	int i=0;
	for(i=0;i<HASH_TABLE_SIZE;i++) {
		locks[i] = 1;
	}
}
/*
*local low level lock based on compare and swap to ensure atomicity 
*/

static int lll_lock(int* lll_val) {
        while(1) {
                while(*lll_val!=1) {
                }
                        if(compare_and_swap(lll_val,0,1))  {
                                return;
                        }//spin here
        }
}
/*
*unlocks the low level lock
*/
static int lll_unlock(int* lll_val) {
        *lll_val=1;
}

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
        for(i=0;i<threadCount && end+1 < size;i++) {
                blockSize = remChars/tc;
                if( (start+blockSize-1 ) <= size ) {
                        end=end+blockSize;
                }
                do {
                        newLine = fileContent[end++];
                } while(newLine!='\n' && newLine!='\0');

/*
		if(newLine == '\n')
			printf("Newline found\n");
		else if(newLine == '\0')
			printf("Null found\n");
		else
			printf("Should not be printed\n");

		printf("i:%d End : %d size: %d\n",i,end , size);
*/		end--;
		(fBlocks+i)->sIndex = start;
		(fBlocks+i)->eIndex = end;
		remChars=size-end;

		tc--;
		start=end+1;
        }

        return size;
}
int hashFunc(int year,char *country) {

	
	int hashVal=0,i=0;
/*	for(i=0; i<strlen(country); i++) {
		hashVal += country[i] * (i+1) ;
	}

	hashVal+=year;
	hashVal = hashVal%HASH_TABLE_SIZE;*/

	int cIndex = searchCountry(country);

	hashVal = cIndex*MAX_NUM_YEARS+(year-START_YEAR); 
	return hashVal;	
}
/*
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
				if (hashTable[index].begin == NULL)
					hashTable[index].end = NULL;
                                free(temp);
//                                return true;
				continue;
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
*/
/* Pseudocode
	updateEntry = createUpdateEntry(); // for updating movieInfo member
	newNode = createNewNode();
	index = findHashIndex();
	
	while(1) {
		curHead = hash[index];		
		if(curHead == NULL) {
			if(CAS(hash[index]->movieInfo,
		}
	}


*/

//bool insert(int index,char *movie,char *country,long int numVotes,int rating,int year) {
bool insert(char *movie,char *country,char *numVotes,char *rating,char *year , int cIndex) {
        bool success = false;

	bool newFlag = false;
	bool updateFlag = false;

	

	long int nVotes = atol(numVotes);
	int r = atoi(rating);
	int yr = atoi(year);
	int index;
	index = hashFunc(yr,country);

	int yrIndex = yr-START_YEAR;
	bool done = false;
	int tmp;
	while(done == false) {
		tmp = count[cIndex][yrIndex];
		if(compare_and_swap(&count[cIndex][yrIndex],tmp+1,tmp) == tmp )
			done = true;
	}

	struct movieInfo *m1 = NULL ,  *oldVal=NULL;
	m1 = malloc(sizeof(struct movieInfo));
	if(m1 == NULL) {
		fprintf(stderr,"insert : malloc failed");
		return false;
	}

	// m1 is used to update the entry with higher rated movie
        strcpy(m1->movie , movie);
        m1->numVotes = nVotes;
        m1->rating = r ;


	// new is used in case of new element in the hash index or same rated entry added in the collision linked list	
	Node *new = NULL;
	new = malloc(sizeof(Node));
	if(new == NULL) {
		fprintf(stderr,"insert: Malloc failed");
		return false;
	}

	strcpy(new->country , country);
	new->m = m1;
        new->year = yr ;
	new->next = NULL;

	Node *temp = NULL;
	Node *curHead = NULL;

	while(1) {
		curHead = hashTable[index];
		if(curHead==NULL ) {	
			if(compare_and_swap_ptr(&hashTable[index],new,NULL) == NULL) {
#ifdef VERBOSE	
				printf("New node inserted in empty bin: %s\n",movie);
#endif
				return;
			} else {
#ifdef VERBOSE	
				printf("New node inserted in empty bin failed: %s\n",movie);
#endif
				continue;
			}
		} else {
			oldVal = curHead->m;
			if(oldVal->rating < r || ( oldVal->rating == r && oldVal->numVotes < nVotes) ) {
				if(compare_and_swap_ptr( &(curHead->m),m1,oldVal) == oldVal ) {
#ifdef VERBOSE  
	                                printf("Current head updated: %s\n",movie);
#endif

					if(curHead->next != NULL) {
						lll_lock(&locks[index]);
						    curHead = hashTable[index];
						    if (curHead->next != NULL ) {
						        temp = curHead->next;
						        curHead->next = NULL;
							// delete the temp linked list	

						    }
						lll_unlock(&locks[index]);
					}
					return;
				} else {
#ifdef VERBOSE  
	                                printf("Current head updation failed:%s\n",movie);
#endif
					continue;
				}
				
			} else if ( oldVal->rating == r && oldVal->numVotes == nVotes ) {

#ifdef VERBOSE  
	                        printf("Collision found:%s\n",movie);
#endif


				lll_lock(&locks[index]);
					curHead = hashTable[index];
					oldVal  = curHead->m;
					
					if ( oldVal->rating == r && oldVal->numVotes == nVotes ) {

						while ( curHead->next != NULL ) {
							curHead = curHead->next;
						}
						curHead->next = new;
					}
					// add to the list
				lll_unlock(&locks[index]);


				
				return;	
/*
				temp = curHead;
				while( ( temp->m->rating == r && temp->m->numVotes == nVotes) || temp->next != NULL ) {
					temp = temp->next;
					
				}
				
				if(temp->m->rating != r  ||  temp->m->numVotes != nVotes) {
					
				} else if ( temp->m->rating == r && temp->m->numVotes == nVotes) { // some other thread have modified it , after this thread came out of the above while loop
					continue;
				} else if ( hashTable[index]->m->rating > temp->m->rating || ( hashTable[index]->m->rating == r && hashTable[index]->m->nVotes > nVotes )) { // head is updated by some one else
					break;
				} else if ( temp->next == NULL ){
					
				}

*/
			} else { // lesser rated movie
				return;
			}

		}  		
	}

 
        return true;
}

int getMax( int *arr , int len  ) {
	int max=0,index=-1;
	int i=0;
	for(i=0;i<len;i++) {
		if(max < arr[i] ) {
			max = arr[i];
			index = i;
		}
			
	}
	return index;
	
}

void printList(Node *n) {
	Node *p;
	p = n;
	while(p != NULL) {
		printf("%s:%ld:%d:%d:%s\n",p->m->movie,p->m->numVotes,p->m->rating,p->year,p->country);
		p=p->next;
	}


}

void printFunc() {
	int i=0,j=0;
	Node *p;

	int max;
	int year;
	int hashIndex,maxIndex=-1;
	int maxRating=-1,maxVotes=-1;
	Node *n;
	for (i=0; i< numCountries ; i++ ) {
		printf("%s:\n",countries[i]);
		
		max = getMax(count[i],MAX_NUM_YEARS);
		while( max!= -1 ) {

#ifdef VERBOSE
			printf("Max num movies index : %d\n",max);
#endif
			
			year = START_YEAR + max;
			printf("%d:%d:\n",year,count[i][max] );
			hashIndex = hashFunc(year,countries[i]);
			printList(hashTable[hashIndex]);
			count[i][max] = 0;
			max = getMax(count[i] , MAX_NUM_YEARS);
		}
		printf("\n");
			
	}

	max = getMax(totalCount,MAX_NUM_YEARS);

	while(max != -1) {
		year = START_YEAR + max;

		printf("%d:%d:\n",year,totalCount[max]);
		for(j=0;j<numCountries;j++) {
			hashIndex = hashFunc(year,countries[j]);
			n = hashTable[hashIndex];
			if ( n != NULL ) {
#ifdef DEBUG
				printf("hashIndex %d : year : %d country %s\n",hashIndex,year,countries[j]);
				printList(n);
#endif

				if(maxRating < n->m->rating || ( maxRating == n->m->rating && maxVotes < n->m->numVotes ) ) {
					maxRating = n->m->rating;
					maxVotes = n->m->numVotes;
				}

			}
				
		}

#ifdef DEBUG
		printf("For year %d , max rating : %d , max votes : %d\n",year , maxRating,maxVotes);
#endif

                for(j=0;j<numCountries;j++) {
                        hashIndex = hashFunc(year,countries[j]);
			n = hashTable[hashIndex];

			if ( n != NULL ) {
				if(  maxRating == n->m->rating  &&  maxVotes == n->m->numVotes  ) {
					printList(hashTable[hashIndex]);
				}
			}

		}		
		totalCount[max] = 0; 
		maxRating=-1;
		maxVotes =-1;
		max = getMax(totalCount,MAX_NUM_YEARS);
	} 

#ifdef DEBUG
	for(i=0;i< HASH_TABLE_SIZE; i++ ) {
		p=hashTable[i];
		while(p != NULL) {
			printf("%d => %s:%s:%ld:%d:%d\n",i,p->m->movie,p->country,p->m->numVotes,p->m->rating,p->year);
			p=p->next;
		}
	}
#endif

/*
	int j=0;
	for(i=0;i<numCountries;i++) {
		for(j=0; j < MAX_NUM_YEARS ;j++) {
			if(count[i][j]!=0) {
				printf("Country : %s Num movie in year %d : %d\n",countries[i], START_YEAR+j,count[i][j]);
			}
		}
	}
*/
}

void getTotalCount() {
	int j=0,i=0;
	for(j=0;j < MAX_NUM_YEARS;j++) {
		for(i=0;i< numCountries; i++ ) {
			totalCount[j] += count[i][j]; 
		}
	}

}

void *threadFunc( void *info ) {
	ThreadInfo *tInfo = (ThreadInfo *)info;


#ifdef DEBUG
	printf("Country:%s ThreadNum:%d\n",countries[tInfo->countryIndex],tInfo->threadId);
#endif
	int index = tInfo->threadId;
	int cIndex = tInfo->countryIndex;
	char movie[MAX_MOVIE_LEN];
	char country[MAX_COUNTRY_LEN];
	char votes[10];
	char rating[5];
	char year[5];
	
	int start = offsets[cIndex][index].sIndex;
	int end = offsets[cIndex][index].eIndex;

	if(start == 0 && end == 0)
		return;
	int i=0;
	do {

//		pthread_mutex_lock(&debugLock);
//		pthread_mutex_unlock(&debugLock);
		//Fetch Movie Name
		while(buffer[cIndex][start] != ':' ) {
			movie[i] = buffer[cIndex][start];
			i++;
			start++;
		}
		movie[i] = '\0';
		i=0;
		start++;

		//Fetch number of votes
                while(buffer[cIndex][start] != ':' ) {
                        votes[i] = buffer[cIndex][start];
                        i++;
                        start++;
                }
                votes[i] = '\0';
                i=0;
		start++;

		//Fetch rating
                while(buffer[cIndex][start] != ':' ) {
                        rating[i] = buffer[cIndex][start];
                        i++;
                        start++;
                }
                rating[i] = '\0';
                i=0;
                start++;


		//Fetch year
                while(buffer[cIndex][start] != ':' ) {
                        year[i] = buffer[cIndex][start];
                        i++;
                        start++;
                }
                year[i] = '\0';
                i=0;
                start++;

		//Fetch country
                while(buffer[cIndex][start] != '\n' && start <= end ) {
                        country[i] = buffer[cIndex][start];
                        i++;
                        start++;
                }
                country[i] = '\0';
                i=0;
		start++;
	
#ifdef DEBUG
		printf("=================\n");
		printf("Movie:%s\nVotes:%s\nRating:%s\nYear:%s\nCountry:%s\n",movie,votes,rating,year,country);
#endif			

		insert(movie,country,votes,rating,year,cIndex);

	} while( start<end ); 	

}

int main() {


	init_locks();
	numCountries = getCountries("countries.txt");
	fillBuffer("p4-in.txt");

	int i=0,j=0;
#ifdef DEBUG
	printf("==========Buffer Contents===========\n");
	for(i=0; i<numCountries; i++) {
		printf("%s",buffer[i]);
	}
#endif


//	char *toChk = malloc(1024 * 1024 * 100);
	
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
/*
	insert(0,"movie","country","90000","7","2009");
	insert(0,"movie","country","90000","8","2009");
	insert(0,"movie1","country1","90000","7","2009");

//	insert(0,"movie2","country2",90000,7,2009);
//	insert(1,"movie2","country2",90000,7,2009);

	print();


	printf("hash of india ,1965 : %d\n",hashFunc(1965,"india"));
	printf("hash of USA ,1965 : %d\n",hashFunc(1965,"USA"));
	printf("hash of china ,1965 : %d\n",hashFunc(1965,"china"));
	printf("hash of japan ,1965 : %d\n",hashFunc(1965,"japan"));
	printf("hash of napaj ,1965 : %d\n",hashFunc(1965,"napaj"));*/


	ThreadInfo tInfo[MAX_NUM_COUNTRY][NUM_THREADS_PER_COUNTRY];
	pthread_t tid[MAX_NUM_COUNTRY][NUM_THREADS_PER_COUNTRY];

	for(i=0;i<numCountries;i++) {
		for(j=0;j<NUM_THREADS_PER_COUNTRY;j++) {
			tInfo[i][j].countryIndex = i ;
			tInfo[i][j].threadId = j;
			pthread_create(&tid[i][j],NULL,threadFunc,(void*)&tInfo[i][j] );
		}

	}


        for(i=0;i<numCountries;i++) {
                for(j=0;j<NUM_THREADS_PER_COUNTRY;j++) {
			pthread_join(tid[i][j],NULL);
		}
	}


	getTotalCount();
	printFunc();
	
}
