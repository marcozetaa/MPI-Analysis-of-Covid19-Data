#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_COUNTRYNAME_LENGTH 20
#define TOP_N 10

typedef struct {
    char* countryName;
    float percentageIncreaseMA;
    int indexInArray; //used only in the min (of the top_n)
} Top10;

typedef struct {
    char* countryName;
    float percentageIncreaseMA;
} CountryResults;

typedef struct {
    int count;
    CountryResults* countries;
} MasterData;
/*-----------------FUNCTIONS-------------*/
char* getfield(char* line, int num) {
    char* aux;
    char* tok;

    aux = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH*2);
    strcpy(aux,line);

    //printf("[NODE %d] Filed took = %s\n",rank,aux);

    // Returns first token
    tok = strtok(aux, ",");
 
    // Keep printing tokens while one of the delimiters present in aux.
    while (tok != NULL && --num!=0) {
        //printf(" %s\n", tok);
        tok = strtok(NULL, ",");
    }

    return tok;
}
/*------------------MAIN---------------*/
int main(){
    Top10 top[TOP_N];
    Top10 min;
    FILE *file;
    MasterData masterData;
    char* line = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH*2);

    //initialise masterdata
    masterData.countries = malloc(sizeof(CountryResults)*20);
    for(int i=0;i<20;i++){
        masterData.countries[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
    }
    masterData.count = 0;

    //read file and set the data
    file = fopen("testinput.txt","r");
    int index=0;
    while(fgets(line,MAX_COUNTRYNAME_LENGTH*2,file) != NULL){
        strcpy(masterData.countries[index].countryName,getfield(line,1));
        masterData.countries[index].percentageIncreaseMA = atof(getfield(line,2));
        masterData.count++;
        index++; 
    }

    for(int i=0;i<index;i++){
        printf("\nCountry: %s\tPercentageIncrease: %f",masterData.countries[i].countryName,masterData.countries[i].percentageIncreaseMA);
    }
    
    printf("\n\n");

    //TOP10 ALGORITHM
    //first for loop is to init and initially fill the array
    min.countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
    min.percentageIncreaseMA = 9999.9;
    for(int i=0;i<TOP_N;i++){
        //initialisation
        top[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
        top[i].percentageIncreaseMA = 0;

        //set the received parameters
        strcpy(top[i].countryName,masterData.countries[i].countryName);
        top[i].percentageIncreaseMA = masterData.countries[i].percentageIncreaseMA;

        //check for the min of the current top10
        if(top[i].percentageIncreaseMA < min.percentageIncreaseMA){
            strcpy(min.countryName,top[i].countryName);
            min.percentageIncreaseMA = top[i].percentageIncreaseMA;
            min.indexInArray = i;
        }
    }

    //second for loop goes through the rest of the countries
    for(int i=TOP_N;i<masterData.count;i++){
        if(masterData.countries[i].percentageIncreaseMA > min.percentageIncreaseMA){
            //update min
            strcpy(min.countryName,masterData.countries[i].countryName);
            min.percentageIncreaseMA = masterData.countries[i].percentageIncreaseMA;
            //update top array
            strcpy(top[min.indexInArray].countryName,min.countryName);
            top[min.indexInArray].percentageIncreaseMA = min.percentageIncreaseMA;

            //update the min index
            for(int j=0;j<TOP_N;j++){
                if(top[j].percentageIncreaseMA < min.percentageIncreaseMA){
                    strcpy(min.countryName,top[j].countryName);
                    min.percentageIncreaseMA = top[j].percentageIncreaseMA;
                    min.indexInArray = j;
                }
            }
        }
    }

    //sorting algorithm - bubble sort
    //TODO: better sorting algorithm
    Top10 tmp;
    tmp.countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
    for(int i=0;i<TOP_N;i++){
        for(int j=i;j<TOP_N;j++){
            if(top[i].percentageIncreaseMA < top[j].percentageIncreaseMA){
                strcpy(tmp.countryName,top[i].countryName);
                tmp.percentageIncreaseMA = top[i].percentageIncreaseMA;

                strcpy(top[i].countryName,top[j].countryName);
                top[i].percentageIncreaseMA = top[j].percentageIncreaseMA;

                strcpy(top[j].countryName,tmp.countryName);
                top[j].percentageIncreaseMA = tmp.percentageIncreaseMA;
            }
        }
    }

    printf("\n\n\nTOP10\n");
    for(int i=0;i<TOP_N;i++){
        printf("%d) %s -> %f\n",i+1,top[i].countryName,top[i].percentageIncreaseMA);
    }

    //clean-up
    fclose(file);
    free(line);
    free(tmp.countryName);
    for(int i=0;i<TOP_N;i++){
        free(top[i].countryName);
    }
    for(int i=0;i<masterData.count;i++){
        free(masterData.countries[i].countryName);
    }


    return 0;
}