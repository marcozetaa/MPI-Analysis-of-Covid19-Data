typedef struct {
    int day, month, year;
    int cases,deaths;
    char* countryterritoryCode; //lo salviamo per avere una key per i selection
}dati;

typedef struct {
    char* countryName;
    /*...dati vari...*/
    dati datiininput[365];
    int i;
} Country;

// Hash table
typedef struct {
    int count; //number of countries
    Country* countries;
    char* top10[10];

} SlaveData;




typedef struct {
    char* countryName;
    /*...dati aggregati...*/
} CountryResults;

// Hash table
typedef struct {
    int count; //number of countries
    CountryResults* countries;
} SlaveData;



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



getIndex();