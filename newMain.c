/*SPECIFICHE: il master invia uno per uno le righe -> quando vede che è cambiato il paese, manda allo slave
un messaggio con scritto "end". Quando poi il master finisce di mandare a tutti gli slave, allora manda a tutti
un messaggio con scritto "totalend"*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <stdbool.h>

#define MAX_LINE_LEN 1024
#define NUM_COUNTRIES 214

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
    int cases,deaths;
    //char* countryterritoryCode; //lo salviamo per avere una key per i selection?
}data;

typedef struct {
    char* countryName;
    data inputData[365]; //saves all the data in the file
    int index;  //used during data receival and computations: keeps count of where in inputData we are at
} Country;

// Hash table
typedef struct {
    int count; //number of countries in the slave
    Country* countries;
    //char* top10[10]; //TODO

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
    //Country* c = slaveData->countries;
    int c = slaveData->count;
    int ind = slaveData->countries[slaveData->count].index;

    strcpy(slaveData->countries[c].countryName, getfield(line,7,rank));

    slaveData->countries[c].inputData[ind].day = atoi(getfield(line,2,rank));
    slaveData->countries[c].inputData[ind].month = atoi(getfield(line,3,rank));
    slaveData->countries[c].inputData[ind].year = atoi(getfield(line,4,rank));
    slaveData->countries[c].inputData[ind].cases = atoi(getfield(line,5,rank));
    slaveData->countries[c].inputData[ind].deaths = atoi(getfield(line,6,rank));

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
    int countriesPerSlave = (NUM_COUNTRIES/num_slaves) + 1; //+1 just to round up

    SlaveData slaveData;

    
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

        // TODO: gestire il ritorno dei valori che poi andranno stampati e usati per il calcolo della top 10


    } else { //slaves
        bool end = false; //flag to identify when the slave has received all the data from 1 country
        bool totalEnd = false; //flag to identify when the data transfer from the master is over (no more countries to be received)
        char line[MAX_LINE_LEN]; // Buffer to receive the line
        int countryIndex; //to know where we are in the index

        /*float buffer[DAYS] = {0}; // circular buffer
        int index = 0; // index of the oldest element in the buffer
        int count = 0; // number of elements in the buffer
        float value;*/

        slaveData.count = 0;
        slaveData.countries = malloc(sizeof(Country)*countriesPerSlave);
        for(int i=0;i<countriesPerSlave;i++){
            slaveData.countries[i].countryName = malloc(sizeof(char)*50);
            slaveData.countries[i].index = 0;
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


            //TODO: estrarre i valori utili dall'input, calcolare il moving average e la percentage ed inserire la entry dentro l'hash table con la key [country]

            // Idea di un calcolo di moving average, usa un buffer circolare di size DAYS che ogni volta calcola
            // la media e la aggiorna ad ogni nuovo valore
            
            
            /*buffer[index] = value;
            index = (index + 1) % DAYS; // wrap around to the beginning if we reach the end
            count = count < DAYS ? count + 1 : DAYS; // keep track of the number of elements

            // calculate the moving average
            float sum = 0;
            for (int i = 0; i < count; i++) {
                sum += buffer[(index + i) % DAYS];
            }
            float average = sum / count;
            printf("Moving average: %.2f\n", average);*/


            //TODO: Calcolo dei top 10 ranking (oppure mandare un messaggio "END" e successivamente calcolarla?


        }
    }

    MPI_Finalize(); // Finalize MPI
    return 0;
}