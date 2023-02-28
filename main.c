#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE_LEN 1024
#define DAYS 5

//Hash table entry
typedef struct {
    char* key;
    int value;
} Entry;

// Hash table
typedef struct {
    int count;
    int size;
    Entry* entries;
} HashTable;

// creation of hash key
unsigned long create_key(char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++) != '\0') {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

// If the key is not in the table, it dinamically creates a new entry
// and if the size of the entry reach the 70% of the total size
// id dinamically doubles the size of the hash table in order to
// avoid a bigger number of collision
void hash_table_put(HashTable* ht, const char* key, int value) {

    if (ht->entries == NULL) {
        ht->size = 100; // Value to set so that the number of collision is minimum
        ht->count = 0;
        ht->entries = (Entry*) malloc(ht->size * sizeof(Entry));
        memset(ht->entries, 0, ht->size * sizeof(Entry));
    }

    unsigned long hash = create_key(key);
    int index = hash % ht->size;
    int i = 1;

    while (ht->entries[index].key != NULL) {
        if (strcmp(ht->entries[index].key, key) == 0) {
            ht->entries[index].value = value;
            return;
        }

        index = (hash + i * i) % ht->size;
        i++;
    }

    ht->entries[index].key = strdup(key);
    ht->entries[index].value = value;
    ht->count++;

    if (ht->count >= 0.7 * ht->size) {
        int new_size = 2 * ht->size;
        Entry* new_entries = (Entry*) calloc(new_size, sizeof(Entry));

        for (int i = 0; i < ht->size; i++) {
            if (ht->entries[i].key != NULL) {
                unsigned long hash = create_key(ht->entries[i].key);
                int index = hash % new_size;
                int j = 1;

                while (new_entries[index].key != NULL) {
                    index = (hash + j * j) % new_size;
                    j++;
                }

                new_entries[index].key = ht->entries[i].key;
                new_entries[index].value = ht->entries[i].value;
            }
        }

        free(ht->entries);
        ht->entries = new_entries;
        ht->size = new_size;
    }
}

int hash_table_get(HashTable* ht, const char* key) {
    unsigned long hash = create_key(key);
    int index = hash % ht->size;
    int i = 1;

    while (ht->entries[index].key != NULL) {
        if (strcmp(ht->entries[index].key, key) == 0) {
            return ht->entries[index].value;
        }

        index = (hash + i * i) % ht->size;
        i++;
    }

    return -1;
}

// get csv columns[num]
const char* getfield(char* line, int num) {

    char* aux = line;
    const char* tok;

    printf("Filed took = %s\n",aux);

    for (tok = strtok(aux, ",");
         tok && *tok;
         tok = strtok(NULL, ",\n"))
    {
        if (!--num)
            return tok;
    }
    return NULL;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv); // Initialize MPI

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Rank of the current process
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Total number of processes

    int num_slaves = size - 1;

    if (rank == 0) { // If the current process is the master

        FILE *fp = fopen("/home/marco/MPI/project/files/input.csv", "r");
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

            strcpy(str, getfield(line,9)); // Extract the field "Country" from the line

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


    } else { // If the current process is a slave
        char line[MAX_LINE_LEN]; // Buffer to receive the line

        float buffer[DAYS] = {0}; // circular buffer
        int index = 0; // index of the oldest element in the buffer
        int count = 0; // number of elements in the buffer
        float value;

        while (1) { // Loop indefinitely
            MPI_Recv(line, MAX_LINE_LEN, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (strncmp(line, "end", 3) == 0) { // If the line is "end"
                break;
            }

            printf("[Slave %d] received line: %s\n", rank - 1, line);

            //TODO: estrarre i valori utili dall'input, calcolare il moving average e la percentage ed inserire la entry dentro l'hash table con la key [country]

            // Idea di un calcolo di moving average, usa un buffer circolare di size DAYS che ogni volta calcola
            // la media e la aggiorna ad ogni nuovo valore
            buffer[index] = value;
            index = (index + 1) % DAYS; // wrap around to the beginning if we reach the end
            count = count < DAYS ? count + 1 : DAYS; // keep track of the number of elements

            // calculate the moving average
            float sum = 0;
            for (int i = 0; i < count; i++) {
                sum += buffer[(index + i) % DAYS];
            }
            float average = sum / count;
            printf("Moving average: %.2f\n", average);


            //TODO: Calcolo dei top 10 ranking (oppure mandare un messaggio "END" e successivamente calcolarla?


        }
    }

    MPI_Finalize(); // Finalize MPI
    return 0;
}



