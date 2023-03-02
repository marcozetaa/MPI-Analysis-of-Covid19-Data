/*SPECIFICHE: il master invia uno per uno le righe -> quando vede che è cambiato il paese, manda allo slave
un messaggio con scritto "end". Quando poi il master finisce di mandare a tutti gli slave, allora manda a tutti
un messaggio con scritto "totalend"
(TODO LATO MASTER)*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <stdbool.h>

#define MAX_LINE_LEN 1024
#define NUM_COUNTRIES 214
#define MAX_COUNTRYNAME_LENGTH 50

/*------------------Master's data structures--------------*/
typedef struct {
    char* countryName;
    //TODO: here we'll save the aggregated data for the country
} CountryResults;

// Hash table
typedef struct {
    int count; //total number of countries
    CountryResults* countries;
} MasterData;

/*------------------Slaves' data structures--------------*/
typedef struct {
    int day, month, year;
    int cases;
    //char* countryterritoryCode; //lo salviamo per avere una key per i selection?
}data;

typedef struct {
    char* countryName;
    data inputData[365]; //saves all the data in the file
    int index;  //used during data receival and computations: keeps count of where in inputData we are at

    float movingAverage; //7-days moving average
    float percentageIncreaseMA;
    int window[7];       //values on which the moving average is computed
    int windowIndex; //next index to use in the sliding window
} Country;

// Hash table
typedef struct {
    int count; //number of countries in the slave
    Country* countries;
    char* top10[10];
} SlaveData;

/*---------------------------FUNCTIONS--------------------*/
// get csv columns[num]
const char* getfield(char* line, int num,int rank) {

    char* aux;
    const char* tok;

    aux = malloc(sizeof(char)*MAX_LINE_LEN);
    strcpy(aux,line);

    printf("[NODE %d] Filed took = %s\n",rank,aux);

    for (tok = strtok(aux, ",");
         tok && *tok;
         tok = strtok(NULL, ",\n"))
    {
        if (!--num)
            return tok;
    }
    return NULL;
}

//save the received data inside the slave's struct
void setData(char* line, SlaveData* slaveData,int rank){
    int c = slaveData->count;
    int ind = slaveData->countries[slaveData->count].index;

    strcpy(slaveData->countries[c].countryName, getfield(line,7,rank));

    slaveData->countries[c].inputData[ind].day = atoi(getfield(line,2,rank));
    slaveData->countries[c].inputData[ind].month = atoi(getfield(line,3,rank));
    slaveData->countries[c].inputData[ind].year = atoi(getfield(line,4,rank));
    slaveData->countries[c].inputData[ind].cases = atoi(getfield(line,5,rank));

    return;
}

// creation of hash key
unsigned long create_key(char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++) != '\0') {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}
/*---------------------------MAIN--------------------*/
int main(int argc, char **argv) {
    MPI_Init(&argc, &argv); // Initialize MPI

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Rank of the current process
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Total number of processes

    int num_slaves = size - 1;
    int countriesPerSlave = (NUM_COUNTRIES/num_slaves) + 1; //+1 needed to avoid round-down problems

    SlaveData slaveData;

    int day, month, year;
    //TODO: these will be updated by the master in order to have the days proceed "synchronously" between all the slaves.
        //This is required since we have to compute a top10 for each day (done by the master)

    
    if (rank == 0) { //master

        FILE *fp = fopen("files/input.csv", "r");
        if (fp == NULL) {
            printf("[MASTER] Error: cannot open file.\n");
            exit(1);
        }

        char line[MAX_LINE_LEN];
        char str[MAX_LINE_LEN];
        int last_sent[num_slaves]; // Index of the last slave process each string was sent to
        memset(last_sent, -1, sizeof(last_sent));

        fgets(line, MAX_LINE_LEN, fp); //Skip first line

        while (fgets(line, MAX_LINE_LEN, fp) != NULL) { // Read each line of the file

            char* buffer = strdup(line);

            strcpy(str, getfield(line,9,rank)); // Extract the field "Country" from the line

            printf("Key Word taken = %s\n",str);

            int dest; // Destination process for the current line
            if (last_sent[create_key(str) % num_slaves] == -1) { // If the string has not been sent before
                dest = (create_key(str) % num_slaves) + 1; // Send it to the corresponding slave process
            } else { // If the string has been sent before
                dest = last_sent[create_key(str) % num_slaves] + 1; // Send it to the same slave process as before
                if (dest > num_slaves) {
                    dest = 1;
                }
            }

            printf("[MASTER] Sending line = %s to process %d\n",buffer,dest);

            MPI_Send(buffer, MAX_LINE_LEN, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
            last_sent[create_key(str) % num_slaves] = dest - 1; // Update the index of the last slave process each string was sent to

        }

        for( int i=1; i<=size; i++){
            MPI_Send("end", MAX_LINE_LEN, MPI_CHAR, i, 0, MPI_COMM_WORLD); // Send the end line to the destination process
        }

        fclose(fp); // Close the input file
    } else { //slaves
        bool end = false; //flag to identify when the slave has received all the data from 1 country
        bool totalEnd = false; //flag to identify when the data transfer from the master is over (no more countries to be received)
        char line[MAX_LINE_LEN]; // Buffer to receive the line
        int countryIndex; //to know where we are in the index

        //variables initialization
        slaveData.count = 0;
        slaveData.countries = malloc(sizeof(Country)*countriesPerSlave);
        for(int i=0;i<10;i++){
            slaveData.top10[i] = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
        }
        for(int i=0;i<countriesPerSlave;i++){
            slaveData.countries[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
            slaveData.countries[i].index = 0;
            for(int j=0;j<7;j++){
                slaveData.countries[i].window[j] = -1;
            }
            slaveData.countries[i].windowIndex = 0;
        }

        while (!totalEnd) { //Loop until all countries are received ("data retrieval loop")
            while(!end){ //Loop until all the data from a specific country is received ("country loop")
                MPI_Recv(line, MAX_LINE_LEN, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                if (strncmp(line, "end", 3) == 0) { // If the line is "end" -> exit from the country loop
                    end=true;
                } 
                else if (strncmp(line, "totalend", 8) == 0) { // If the line is "totalend" -> exit from the data retrieval loop
                    totalEnd=true;
                    end = true;
                } else {
                    printf("[Slave %d] received line: %s\n", rank, line);
                    
                    //set the data into the structs
                    setData(line,&slaveData,rank);
                    slaveData.countries[slaveData.count].index++;
                }
            }

            slaveData.count++;
            end = false;
        }

        //reset the index
        for(int i=0;i<slaveData.count;i++){
            slaveData.countries[i].index = 0;
        }
    }

    /*HERE FINISHES THE DATA DISTRIBUTION PART; FROM HERE THERE ARE THE ACTUAL COMPUTATIONS*/

    if(rank==0){ //master
        // TODO: gestire il ritorno dei valori che poi andranno stampati e usati per il calcolo della top 10
    } else { //slaves
        //Moving average computation and percentage increase computation
        for(int i=0;i<slaveData.count;i++){ //for each country
            float newMovingAverage = 0.0;
            int consideredDays = 0;
            Country* country = &slaveData.countries[i];
            country->window[country->windowIndex] = country->inputData[country->index].cases;   //overwrites one cell of the circular buffer
            
            //update the indexes
            country->index++;
            country->windowIndex = (country->windowIndex+1)%7;  //keeps the window buffer circular

            for(int j=0;j<7;j++){
                if(country->window[j]!=-1) {    //-1 is the initialization value. Needed to correctly compute the 1st 6 days
                    newMovingAverage += (float) country->window[j];
                    consideredDays++;
                }
            }
            newMovingAverage/= (float) consideredDays;

            country->percentageIncreaseMA = newMovingAverage/country->movingAverage;
            country->movingAverage = newMovingAverage;


            //convert values into a single string, separated by ","
            char* stringToSend = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH*2);
            strcpy(stringToSend,"");
            char* tmp = malloc(sizeof(char)*10);

            strcat(stringToSend,country->countryName);
            strcat(stringToSend,",");

            gcvt(country->movingAverage,8,tmp);
            strcat(stringToSend,tmp);
            strcat(stringToSend,",");
            
            gcvt(country->percentageIncreaseMA,8,tmp);
            strcat(stringToSend,tmp);
            strcat(stringToSend,",");


            MPI_Send(stringToSend,strlen(stringToSend),MPI_CHAR,0,0,MPI_COMM_WORLD);
        }

        //TODO
        //Top ten computation
        int top10indexes[10]; //indexes in slaveData.countries[] of the top10 countries
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize(); // Finalize MPI
    return 0;
}