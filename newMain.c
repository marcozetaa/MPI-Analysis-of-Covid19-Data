/*SPECIFICHE: il master invia uno per uno le righe -> quando vede che è cambiato il paese, manda allo slave
un messaggio con scritto "end". Quando poi il master finisce di mandare a tutti gli slave, allora manda a tutti
un messaggio con scritto "totalend"*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <stdbool.h>

#define MAX_LINE_LEN 2048
#define NUM_COUNTRIES 214
#define MAX_COUNTRYNAME_LENGTH 50

/*------------------Master's data structures--------------*/
typedef struct {
    char* countryName;
    float movingAverage;
    float percentageIncreaseMA;
} CountryResults;

// Entry for master
typedef struct {
    int count; //total number of countries
    CountryResults* countries;
} MasterData;

/*------------------Slaves' data structures--------------*/
typedef struct {
    int day, month, year;
    int cases;
} data;

typedef struct {
    char* countryName;
    data inputData[365]; //saves all the data in the file
    int index;  //used during data receival and computations: keeps count of where in inputData we are at

    float movingAverage; //7-days moving average
    float percentageIncreaseMA;
    int window[7];       //values on which the moving average is computed
    int windowIndex; //next index to use in the sliding window
} Country;

// Entry for slave
typedef struct {
    int count; //number of countries in the slave
    Country* countries;
    char* top10[10];
} SlaveData;

/*---------------------------FUNCTIONS--------------------*/
//get csv columns[num]
//columns are numbered from 1
const char* getfield(char* line, int num,int rank) {

    char* aux;
    const char* tok;

    aux = malloc(sizeof(char)*MAX_LINE_LEN);
    strcpy(aux,line);

    printf("[NODE %d] Filed took = %s\n",rank,aux);

    for (tok = strtok(aux, ","); tok && *tok; tok = strtok(NULL, ",\n")){
        if (!--num){
            free(aux);
            return tok;
        }
    }

    free(aux);
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

//creation of hash key
unsigned long create_key(char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++) != '\0') {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

//computes the next date
void nextDate(int *day, int *month, int *year){
    int daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    *day = *day +1;
    if (( *month == 2 ) && (*day == 29))
    {
        // Leap year checking, if yes, Feb is 29 days.
        if(*year % 400 == 0 || (*year % 100 != 0 && *year % 4 == 0))
        {
            daysInMonth[1] = 29;
        }
    }

    if (*day > daysInMonth[*month -1])
    {
        *day = 1;
        *month = *month +1;
        if (*month > 12)
        {
            *month = 1;
            *year = *year +1;
        }
    }
}

//TODO: è una ricerca binaria -> magari ottimizziamo la funzione cambiando algoritmo
//get the index of the required state from masterData.countries
int getIndex(char *name, MasterData masterData){
    int low = 0;
    int high = masterData.count-1;
    int mid = 0;
    int result = 0;

    while(high!=low){
        mid = (low+high)/2;
        result = strcmp(name,masterData.countries[mid].countryName);
        if(result == 0) return mid;
        else if(result<0) high = mid-1;
        else low = mid+1;;
    }

    return -1;  //error
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
    MasterData masterData;

    int day, month, year;

    if (rank == 0) { //master
        masterData.count = 0;
        masterData.countries = malloc(sizeof(CountryResults)*NUM_COUNTRIES);
        for(int i=0;i<NUM_COUNTRIES;i++){
            masterData.countries[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
            masterData.countries[i].movingAverage = 0.0;
            masterData.countries[i].percentageIncreaseMA = 0.0;
        }


        FILE *fp = fopen("files/input.csv", "r");
        if (fp == NULL) {
            printf("[MASTER] Error: cannot open file.\n");
            exit(1);
        }

        char line[MAX_LINE_LEN];
        char str[MAX_LINE_LEN];

        char* lastCountry = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH); // allocate memory for lastCountry
        int dest = 0;

        fgets(line, MAX_LINE_LEN, fp); //Skip first line

        while (fgets(line, MAX_LINE_LEN, fp) != NULL) { // Read each line of the file

            char* buffer = strdup(line);

            strcpy(str, getfield(line,7,rank)); // Extract the field "Country" from the line

            if(strcmp(lastCountry, str) != 0){ // if the country has changed

                if(masterData.count!=0) MPI_Send("end", MAX_LINE_LEN, MPI_CHAR, dest, 0, MPI_COMM_WORLD); // Send the end line to the destination process

                strcpy(lastCountry, str); // copy the value of str to lastCountry

                dest = (dest + 1) % size == 0 ? 1 : (dest + 1) % size;
                strcpy(masterData.countries[masterData.count].countryName,lastCountry);
                masterData.count++;
            }

            MPI_Send(buffer, MAX_LINE_LEN, MPI_CHAR, dest, 0, MPI_COMM_WORLD);

            free(buffer);
        }

        for( int i=1; i<=size; i++){
            MPI_Send("totalend", MAX_LINE_LEN, MPI_CHAR, i, 0, MPI_COMM_WORLD); // Send the end line to the destination process
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

    day = 30;
    month=12;
    year = 2019;

    while(!(day==14 && month==12 && year==2020)){ //until the end of the dataset

        //moving average and percentage increase
        if(rank==0){ //master
            nextDate(&day,&month,&year);
            char *line = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH*2);
            char *name = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
            char *dateString = malloc(sizeof(char)*11);
            sprintf(dateString, "%d,%d,%d", day,month,year);
            for(int i=0;i<num_slaves;i++){
                MPI_Send(dateString,10,MPI_CHAR,i,3,MPI_COMM_WORLD);
            }
            for(int i=0;i<masterData.count;i++){
                MPI_Recv(line,MAX_COUNTRYNAME_LENGTH*2,MPI_CHAR,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                
                strcpy(name,getfield(line,1,rank)); //get the country name

                int index = getIndex(name,masterData); //look for the correct index in the list and update the values
                masterData.countries[index].movingAverage = atof(getfield(line,2,rank));
                masterData.countries[index].percentageIncreaseMA = atof(getfield(line,3,rank));
            }

            free(name);
            free(line);
        } else { //slaves -> TODO: aggiungere il controllo dei giorni
            //Moving average computation and percentage increase computation
            // TODO Receive date from master
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


                MPI_Send(stringToSend,strlen(stringToSend),MPI_CHAR,0,1,MPI_COMM_WORLD);
            }
        }

        //TODO
        //MUST DO: use tag 2 in the communications
        //Top ten computation
        //int top10indexes[10]; //indexes in slaveData.countries[] of the top10 countries
         if(rank==0){ //master
        } else { //slaves

        }

    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize(); // Finalize MPI
    return 0;
}