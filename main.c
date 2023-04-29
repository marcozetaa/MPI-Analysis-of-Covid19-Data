#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <time.h>
#include "mpi.h"

#define NUM_COUNTRIES 214
#define MAX_LINE_LEN 2048
#define MAX_COUNTRYNAME_LENGTH 50
#define TOP_N 10

extern char* strdup(const char*);

typedef struct {
    int      day, month, year;
    int      cases;
} data;

typedef struct {
    char*    countryName;

    data     inputData[365]; //saves all the data in the file
    int      index;  //used during data receival and computations: keeps count of where in inputData we are at

    float    movingAverage;    //7-days moving average
    float    percentageIncreaseMA;
    int      tot_cases;

    int      window[7];       //values on which the moving average is computed
    int      windowIndex; //next index to use in the sliding window
} Country;

// Entry for slave
typedef struct {
    int      count; //number of countries in the slave
    Country* countries;
} SlaveData;

typedef struct {
    char*   countryName;
    float   percentageIncreaseMA;
    int     cases;
} CountryResults;

// Insertion Sort algorithm
void insertionSort(CountryResults* countries, int country_count){
    CountryResults key;
    int i, j;
    for (i = 1; i < country_count; i++) {
        key = countries[i];
        j = i - 1;

        /* Move elements of arr[0..i-1], that are
          greater than key, to one position ahead
          of their current position */
        while (j >= 0 && countries[j].percentageIncreaseMA > key.percentageIncreaseMA) {
            countries[j + 1] = countries[j];
            j = j - 1;
        }
        countries[j + 1] = key;
    }
}

// Computes the next date
int isLessRecent(int *day1, int *month1, int *year1, int *day2, int *month2, int *year2) {
    if (*year1 < *year2) {
        return 0;
    } else if (*year1 > *year2) {
        return 1;
    } else {
        if (*month1 < *month2) {
            return 0;
        } else if (*month1 > *month2) {
            return 1;
        } else {
            if (*day1 < *day2) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}

// Computes the next date
void nextDate(int *day, int *month, int *year){
    int daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    *day = *day +1;
    if (( *month == 2 ) && (*day == 29)){
        // Leap year checking, if yes, Feb is 29 days.
        if(*year % 400 == 0 || (*year % 100 != 0 && *year % 4 == 0)){
            daysInMonth[1] = 29;
        }
    }

    if (*day > daysInMonth[*month -1]){
        *day = 1;
        *month = *month +1;
        if (*month > 12){
            *month = 1;
            *year = *year +1;
        }
    }
}

//Get csv columns[num] - Columns are numbered from 1
char* getCol(char* line, int num) {
    char* aux;
    char* tok;

    aux = malloc(sizeof(char)*strlen(line)+1);
    strcpy(aux,line);

    // Returns first token
    tok = strtok(aux, ",");

    // Keep printing tokens while one of the delimiters present in aux.
    while (tok != NULL && --num!=0) {
        tok = strtok(NULL, ",");
    }

    return tok;
}

// Prints some important slave data (country name,day/month/year,cases)
void printSlaveData(SlaveData slaveData){
    char* str = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
    int day, month, year, cases;
    int c = slaveData.count;
    int ind = slaveData.countries[slaveData.count].index;

    strcpy(str,slaveData.countries[c].countryName);
    day = slaveData.countries[c].inputData[ind].day;
    month = slaveData.countries[c].inputData[ind].month;
    year = slaveData.countries[c].inputData[ind].year;
    cases = slaveData.countries[c].inputData[ind].cases;

    printf("Line: %s,%d/%d/%d,%d\n",str,day,month,year,cases);
}

// Save the received data inside the slave's struct
void setData(char* line, SlaveData* slaveData){
    int c = slaveData->count;
    int ind = slaveData->countries[slaveData->count].index;

    strcpy(slaveData->countries[c].countryName, getCol(line,4));

    slaveData->countries[c].inputData[ind].day = atoi(getCol(line,1));
    slaveData->countries[c].inputData[ind].month = atoi(getCol(line,2));
    slaveData->countries[c].inputData[ind].year = atoi(getCol(line,3));
    slaveData->countries[c].inputData[ind].cases = atoi(getCol(line,5));

    //printSlaveData(*slaveData);

}

// SlaveData struct initialization
void initializeSlaveData(SlaveData* slaveData, int countries_per_slave){

    //variables initialization
    slaveData->count = 0;
    slaveData->countries = malloc(sizeof(Country)*countries_per_slave);

    for(int i=0;i<countries_per_slave;i++){

        slaveData->countries[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
        slaveData->countries[i].index = 0;
        slaveData->countries[i].tot_cases = 0;

        for(int j=0;j<7;j++){
            slaveData->countries[i].window[j] = -1;
        }

        slaveData->countries[i].windowIndex = 0;
    }

}

// SlaveData struct deletion
void deleteSlaveData(SlaveData* slaveData, int countriesPerSlave){

    for(int i=0;i<countriesPerSlave;i++){
        free(slaveData->countries[i].countryName);
    }

    free(slaveData->countries);
}

// Update country moving_average and increase_percentage
void updateCountry(Country* country, int day, int month, int year){

    float newMovingAverage = 0.0;
    int consideredDays = 0;

    // New moving average is computed only if the current date is present in the dataset of this country
    if(country->inputData[country->index].day == day && country->inputData[country->index].month == month && country->inputData[country->index].year == year) {

        country->window[country->windowIndex] = country->inputData[country->index].cases;   //overwrites one cell of the circular buffer

        //update the indexes
        country->index--;
        country->windowIndex = (country->windowIndex+1)%7;  //keeps the window buffer circular

        for(int j=0;j<7;j++){
            if(country->window[j]!=-1) {    //-1 is the initialization value. Needed to correctly compute the 1st 6 days
                newMovingAverage += (float) country->window[j];
                consideredDays++;
            }
        }

        newMovingAverage/= (float) consideredDays;

        country->percentageIncreaseMA = country->movingAverage!=0.0 ? (newMovingAverage/country->movingAverage) : newMovingAverage;
        //done to avoid dividing by 0 the first time (the 1st time, it goes to newMovingAverage, then it's always the 1st choice)
        country->movingAverage = newMovingAverage;
        country->tot_cases += country->inputData[country->index].cases;
    }
    else {
        //printf("--------------------------------\n");
        //printf("[Slave]: date not present\n");
        //printf("DAY: %d, MONTH:%d, YEAR: %d\n",day,month,year);
        //printf("COUNTRY: %s, INDEX: %d --> Day: %d,Month: %d, Year: %d\n",country->countryName,country->index,country->inputData[country->index].day,country->inputData[country->index].month,country->inputData[country->index].year);
        //printf("--------------------------------\n");
    }

}

// Print the number of case of all countries and then prints the TOP N rank
void printResults(CountryResults* countries, int country_count, int day, int month, int year){

    if(country_count < TOP_N){
        printf("[MASTER] Error: Ranking does not have sufficient countries\n");
        return;
    }

    printf("\n");
    printf("---------------------------------------------------------------\n");

    printf("\nCountries Cases and Percentage Increase %d/%d/%d\n",day,month,year);
    for(int i=0;i<country_count;i++){
        printf("%d) %s: %d Cases -> +%.2f%% \n",i+1,countries[i].countryName,countries[i].cases,countries[i].percentageIncreaseMA);
    }

    printf("\n");
    printf("---------------------------------------------------------------\n");

    insertionSort(countries,country_count);

    printf("\nTOP %d %d/%d/%d\n",TOP_N,day,month,year);
    for(int i=0;i<TOP_N;i++){
        printf("%d) %s: %d Cases -> +%.2f%% \n",i+1,countries[country_count-i-1].countryName,countries[country_count-i-1].cases,countries[country_count-i-1].percentageIncreaseMA);
    }

    printf("\n");
    printf("---------------------------------------------------------------\n");

}

void slave_function(SlaveData* slave_data, int rank, int countries) {
    int country_count = 0;
    int day, month, year;
    char msg[MAX_LINE_LEN], prev_country_name[MAX_COUNTRYNAME_LENGTH];
    char* country_name;

    // ---------------------------DISTRIBUTION PART---------------------------

    // Clean msg
    memset(&msg[0], 0, sizeof(msg));

    while (1) {
        // Receive row from master

        MPI_Recv(msg, MAX_LINE_LEN, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Check for end signal from master
        if (strcmp(msg,"END") == 0) {
            break;
        }

        // Parse row data
        country_name = getCol(msg,4);

        // Check for country change
        if (country_count > 0 && strcmp(country_name, prev_country_name) != 0) {
            // Changing country
            //printf("[NODE %d] Got country %s\n",rank,country_name);
            slave_data->count++;
        }

        // Add row to country data
        setData(msg,slave_data); //set the data into the structs
        slave_data->countries[slave_data->count].index++;

        // Update previous country name
        strcpy(prev_country_name, country_name);

        // Increment row and country counts
        country_count++;

        // Clean msg
        memset(&msg[0], 0, sizeof(msg));
    }

    //get the index ready for reading the data
    for(int i=0;i<slave_data->count;i++){
        slave_data->countries[i].index -= 1;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    //printf("[NODE %d] I have %d countries\n",rank,slave_data->count+1);

    // ---------------------------COMPUTATION PART---------------------------

    int end_day, end_month, end_year;

    // Clean buffer
    memset(&msg[0], 0, sizeof(msg));

    // Receive upper bound of date
    MPI_Recv(msg,MAX_LINE_LEN,MPI_CHAR,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE); // Receive the current date from master
    day = atoi(getCol(msg,1));
    month = atoi(getCol(msg,2));
    year = atoi(getCol(msg,3));


    // Receive lower bound of date
    MPI_Recv(msg,MAX_LINE_LEN,MPI_CHAR,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE); // Receive the current date from master
    end_day = atoi(getCol(msg,1));
    end_month = atoi(getCol(msg,2));
    end_year = atoi(getCol(msg,3));

    // Clean buffer
    memset(&msg[0], 0, sizeof(msg));

    while(1) {
        // Receive date request
        MPI_Recv(msg, MAX_LINE_LEN, MPI_CHAR, 0, 3, MPI_COMM_WORLD,MPI_STATUS_IGNORE); // Receive the current date from master

        day = atoi(getCol(msg, 1));
        month = atoi(getCol(msg, 2));
        year = atoi(getCol(msg, 3));

        for (int i = 0; i <= slave_data->count; i++) {

            Country *country = &slave_data->countries[i];

            //printf("[NODE %d] updating country: %s -  %d out of %d\n",rank,country->countryName,i,slave_data->count);

            updateCountry(country, day, month, year);

            sprintf(msg, "%s,%.2f,%d", country->countryName, country->percentageIncreaseMA,country->tot_cases);

            //printf("[NODE %d] Message is: %s\n",rank,msg);

            MPI_Send(msg,strlen(msg),MPI_CHAR,0,1,MPI_COMM_WORLD);

        }

        // Clean buffer
        memset(&msg[0], 0, sizeof(msg));

        if(day==end_day && month==end_month && year==end_year) break;
    }

    // Clean buffer
    memset(&msg[0], 0, sizeof(msg));

}

void master_function(int num_processes) {
    int slave_rank = 1, start = 0, country_count = 0;
    int day, month, year, start_day = 32, start_month = 13, start_year = 3000, end_day = 1, end_month = 1, end_year = 1;
    char buffer[MAX_LINE_LEN], msg[MAX_LINE_LEN], country_name[50], current_country_name[50] = "";


    // Open input file
    FILE* input_file = fopen("files/input.csv", "r");
    if (input_file == NULL) {
        printf("Error: could not open input file\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // ---------------------------DISTRIBUTION PART---------------------------

    // Read input data and calculate rows per country
    fgets(buffer, MAX_LINE_LEN, input_file);

    while(fgets(buffer,MAX_LINE_LEN,input_file)){
        // Read country name from input data

        char* line = strdup(buffer);

        if(start == 0){
            start_day = atoi(getCol(line, 2));
            start_month = atoi(getCol(line,3));
            start_year = atoi(getCol(line,4));
            start = 1;
        }

        strcpy(country_name, getCol(line,7)); // Extract the field "Country" from the line

        snprintf(msg, sizeof(msg),"%s,%s,%s,%s,%s", getCol(line,2), getCol(line,3), getCol(line,4), country_name, getCol(line,5)); // Preparing send message

        if (strcmp(country_name, current_country_name) != 0) {

            // Switch to next slave
            slave_rank = (slave_rank % num_processes) + 1;

            // Update current country name
            strcpy(current_country_name, country_name);

            // Increment country count and rows read
            country_count++;
        }

        // Send row to slave
        MPI_Send(msg, strlen(msg), MPI_CHAR, slave_rank, 0, MPI_COMM_WORLD);

        day = atoi(getCol(line,2));
        month = atoi(getCol(line,3));
        year = atoi(getCol(line,4));

        // Update the earliest date the process can find
        if(!isLessRecent(&day,&month,&year,&start_day,&start_month,&start_year)){
            start_day = day;
            start_month = month;
            start_year = year;
        }

        // Update the latest date the process can find
        if(isLessRecent(&day,&month,&year,&end_day,&end_month,&end_year)){
            end_day = day;
            end_month = month;
            end_year = year;
        }

    }

    //printf("[MASTER] Tot countries: %d\n",country_count);

    // Close input file
    fclose(input_file);

    // Send end signal to all slaves
    for (int i = 1; i <= num_processes; i++) {
        MPI_Send("END", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // ---------------------------COMPUTATION PART---------------------------

    // Clean msg
    memset(&msg[0], 0, sizeof(msg));

    day = start_day;
    month = start_month;
    year = start_year;

    nextDate(&start_day,&start_month,&start_year);

    snprintf(msg, sizeof(msg), "%d,%d,%d", start_day,start_month,start_year);
    // Send upper bound date to all slaves
    for (int i = 1; i <= num_processes; i++) {
        MPI_Send(msg, strlen(msg), MPI_CHAR, i, 0, MPI_COMM_WORLD);
    }

    snprintf(msg, sizeof(msg), "%d,%d,%d", end_day,end_month,end_year);
    // Send lower bound date  to all slaves
    for (int i = 1; i <= num_processes; i++) {
        MPI_Send(msg, strlen(msg), MPI_CHAR, i, 0, MPI_COMM_WORLD);
    }

    // Clean msg
    memset(&msg[0], 0, sizeof(msg));

    // Prepare data
    CountryResults countries_results[country_count];
    for(int i=0;i<country_count;i++){
        countries_results[i].countryName = malloc(sizeof(char)*MAX_COUNTRYNAME_LENGTH);
    }

    // Collect results from slaves and print top 10 countries
    while(!(day==end_day && month==end_month && year==end_year)){

        snprintf(msg, sizeof(msg), "%d,%d,%d", day,month,year);

        //printf("[CALCULATE] DAY %d/%d/%d\n",day,month,year);

        for(int i=1;i<=num_processes;i++){
            MPI_Send(msg,strlen(msg),MPI_CHAR,i,3,MPI_COMM_WORLD); // send the current date to all the slaves
        }

        for( int i = 0; i < country_count; i++){
            // Clean buffer
            memset(&buffer[0], 0, sizeof(buffer));

            MPI_Recv(buffer,MAX_LINE_LEN,MPI_CHAR,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);

            //printf("[MASTER] Received msg: %s out of %d\n",buffer,i);

            // Save country result
            strcpy(countries_results[i].countryName,getCol(buffer,1));
            countries_results[i].percentageIncreaseMA = atof(getCol(buffer,2));
            countries_results[i].cases = atof(getCol(buffer,3));
        }

        // Clean buffer
        memset(&buffer[0], 0, sizeof(buffer));

        // Print ranking
        printResults(countries_results,country_count,day,month,year);

        nextDate(&day,&month,&year);
    }

    // Last message to send to the slaves
    //printf("[MASTER] Sending Date; %d/%d/%d\n",day,month,year);

    snprintf(msg, sizeof(msg), "%d,%d,%d", day,month,year);

    for(int i=1;i<=num_processes;i++){
        MPI_Send(msg,strlen(msg),MPI_CHAR,i,3,MPI_COMM_WORLD); // send the current date to all the slaves
    }

    for( int i = 0; i < country_count; i++){
        // Clean buffer
        memset(&buffer[0], 0, sizeof(buffer));

        MPI_Recv(buffer,MAX_LINE_LEN,MPI_CHAR,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);

        // Save country result
        strcpy(countries_results[i].countryName,getCol(buffer,1));
        countries_results[i].percentageIncreaseMA = atof(getCol(buffer,2));
    }

    // Clean buffer
    memset(&buffer[0], 0, sizeof(buffer));

    // Print ranking
    printResults(countries_results,country_count,day,month,year);

    for(int i=0;i<country_count;i++){
        free(countries_results[i].countryName);
    }
}

int main(int argc, char **argv) {
    int rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Get initial time (used later to check the elapsed time)
    double starttime = MPI_Wtime();

    int num_slaves = size - 1;
    int countries_per_slave = (NUM_COUNTRIES/num_slaves) + 1; //+1 needed to avoid round-down problems

    if (rank == 0) {
        // Master process
        master_function(num_slaves);
    } else {
        // Slave processes
        SlaveData slave_data;

        // Initialize Slave Process
        initializeSlaveData(&slave_data,countries_per_slave);

        slave_function(&slave_data,rank,countries_per_slave);

        // Free memory
        deleteSlaveData(&slave_data,countries_per_slave);
    }

    // Master computes the elapsed time
    if(rank==0){
        double endtime = MPI_Wtime();
        printf("\n\nElapsed time -> %f milliseconds\n\n",(endtime-starttime)*1000);
    }

    MPI_Finalize();
    return 0;
}
