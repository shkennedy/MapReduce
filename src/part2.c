#include "lott.h"
#include "parts1_2.h"

int part2(size_t nthreads) {
    // Check for invalid input
    if (nthreads < 1) {
        return -1;
    }
    
    // Create linked list of sinfo nodes, nfiles long
    sinfo *head = NULL;
    int nfiles = make_files_list(&head);
    sinfo *cursor = head;

    // Divide sinfo list equally between map threads
    pthread_t t_readers[nthreads];
    margs args[nthreads];
    char threadname[THREADNAME_SIZE];
    int nfiles_per = nfiles / nthreads, nfiles_rem = nfiles % nthreads;
    for (int i = 0; i < nthreads; ++i) {
        
        // Set nfiles
        args[i].nfiles = nfiles_per;
        if (i < nfiles_rem) {
            ++args[i].nfiles;
        }
        if (args[i].nfiles == 0) {
            break;
        }

        // Set new sub-list head
        args[i].head = cursor;

        // Create and name map thread
        pthread_create(&t_readers[i], NULL, map, &args[i]);
        sprintf(threadname, "%s%d", "map", i + 2);
        pthread_setname_np(t_readers[i], threadname);

        // Move to next sub-list head
        if (i + 1 == nthreads) {
            break;
        }
        for (int j = 0; j < args[i].nfiles; ++j) {
            cursor = cursor->next;
        }
    }

    // Join all used map threads 
    for (int i = 0; i < nthreads; ++i) {
        if (args[i].nfiles) {
            pthread_join(t_readers[i], NULL);
        }
    }

    // Find result of query
    head = reduce(head);
    printf(
        "Part: %s\n"
        "Query: %s\n"
        "Result: %lf, %s\n",
        PART_STRINGS[current_part], QUERY_STRINGS[current_query],
        head->average, head->filename);

    // Restore resources
    sinfo *prev;
    cursor = head;
    if (current_query == E) {
        while (cursor != NULL) {
            free(cursor->einfo);     
            prev = cursor;
            cursor = cursor->next;
            free(prev);
        }
    } else {
        while (cursor != NULL) {
            prev = cursor;
            cursor = cursor->next;
            free(prev);
        }
    }

    return 0;
}

/**
* Makes a linked list of sinfo nodes, returns the length of the list
*
* @param head Pointer to sinfo pointer where head pointer will be stored
* @return Number of files found in data dir (length of list created)
*/
static int make_files_list(sinfo **head) {
    int nfiles;

    // Open data directory
    DIR *dir = opendir(DATA_DIR);
    struct dirent *direp;
    
    // For every file found, add a node containing the filename
    for (nfiles = 0; (direp = readdir(dir)) != NULL; ++nfiles) {
        if (direp->d_name[0] == '.') {
            --nfiles;
            continue;
        }
        
        sinfo *new_node = calloc(1, sizeof(sinfo));
        strcpy(new_node->filename, direp->d_name);
        if (current_query == E) {
            new_node->einfo = calloc(CCOUNT_SIZE, sizeof(int));
        }

        new_node->next = *head;
        *head = new_node;
    }

    closedir(dir);
    return nfiles;
}

/**
* Map controller, calls map function for current query,
* Acts as start routine for created threads 
*
* @param v Pointer to margs container for map arguments
*/
static void* map(void* v) {
    margs *args = v;
    sinfo *info = args->head;
    
    // Find map for current query
    void (*f_map)(sinfo*);
    switch (current_query) {
        case A:
        case B:
            f_map = &map_avg_dur;
            break;
        case C:
        case D:
            f_map = &map_avg_user;
            break;
        case E:
            f_map = &map_max_country;
    }

    // For all files assigned to this thread
    char filepath[FILENAME_SIZE];
    sprintf(filepath, "./%s/", DATA_DIR);
    for (int i = 0; i < args->nfiles; ++i) {
       
        // Open File
        strcpy(filepath + 7, info->filename);
        info->file = fopen(filepath, "r");
        if (info->file == NULL) {
            exit(EXIT_FAILURE);
        }

        // Call map for query
        (*f_map)(info);

        // Close file
        fclose(info->file);
        info = info->next;
    }

    pthread_exit(NULL);
    return NULL;
}

// Converts string to integer
static int stoi(char *str, int n) {
    int num = 0;
    for (int i = 0; i < n; ++i) {
        num *= 10;
        num += str[i] - '0';
    }
    return num;
}

// Converts string to long
static long stol(char *str, int n) {
    long num = 0;
    for (int i = 0; i < n; ++i) {
        num *= 10;
        num += str[i] - '0';
    }
    return num;
}

/**
* (A/B) Map function for finding average duration of visit, sets average in 
* passed sinfo node
* 
* @param info Pointer to sinfo node to store average in 
*/
static void map_avg_dur(sinfo *info) {
    char line[LINE_SIZE], *linep = line, *durstr;
    int nvisits = 0, duration = 0;
    
    // For all lines in file
    while (fgets(line, LINE_SIZE, info->file) != NULL) {
        
        // Find duration segment of line
        strsep(&linep, ","), strsep(&linep, ",");
        durstr = strsep(&linep, ",");
       
        // Add duration to total
        duration += stoi(durstr, strlen(durstr));
        linep = line;
        ++nvisits;
    }

    // Find average duration 
    info->average = (double)duration / nvisits;
}

// Helper for map_avg_user
// Checks bit array used_years for previously found years
// Returns 1 if year not found, else 0
static int check_year_used(int year, unsigned long *used_years) {
    // Check bit at offset from 1970
    unsigned long mask = 1;
    year -= 70;
    mask <<= year;
    if (mask & *used_years) {
        // Match found - year used
        return 0;
    }
    // Match not found - year not used
    *used_years |= mask;
    return 1;
}

/**
* (C/D) Map function for finding average users per year, sets average
* in passed sinfo node
*
* @param info Pointer to sinfo node to store average in 
*/
static void map_avg_user(sinfo *info) {
    char line[LINE_SIZE], *linep = line, *timestamp;
    int nvisits = 0, nyears = 0;
    unsigned long used_years = 0;
    time_t ts;
    struct tm tm;
    
    // For all lines in file
    while (fgets(line, LINE_SIZE, info->file) != NULL) {
        
        // Find timestamp segment of line
        timestamp = strsep(&linep, ",");

        // Find year from timestamp
        ts = stol(timestamp, strlen(timestamp));
        localtime_r(&ts, &tm);

        // Add to nyears if year is new
        nyears += check_year_used(tm.tm_year, &used_years);
        ++nvisits;
        linep = line;
    } 
    // Find average users
    info->average = (double)nvisits / nyears;
}

/**
* (E) Map function for finding country count, creates linked list for all 
* countries 
* 
* @param info Pointer to sinfo node to store country counts list in
*/
static void map_max_country(sinfo *info) {
    char line[LINE_SIZE], *linep = line;
    int ind;

    // For all lines in file
    while (fgets(line, LINE_SIZE, info->file) != NULL) {
        
        // Find country code segment of line
        strsep(&linep, ","), strsep(&linep, ","), strsep(&linep, ",");
         
        // Turn country_code into an index
        ind = ((linep[0] - 'A') * 26) + (linep[1] - 'A');

        // Add to count for that country
        ++(info->einfo[ind]);
        linep = line;
    } 
}

/**
* Reduce controller, calls reduce function for current query
* 
* @param v Pointer to head of sinfo linked list
* @return Pointer to sinfo containing result
*/
static void* reduce(void* v) {
    sinfo *head = v;

    // Find reduce for current query
    void* (*f_reduce)(sinfo*);
    if (current_query == E) {
        f_reduce = &reduce_max_country;
    } else {
        f_reduce = &reduce_avg;
    }

    return (*f_reduce)(head);
}

// Helper for reduce_avg_dur and reduce_avg_user
// Returns comparison based on current_query
static char avgcmp(sinfo *a, sinfo *b) {
    if (current_query == A || current_query == C) {
        if (a->average > b->average) {
            return 1;
        } else if (a->average < b->average) {
            return -1;
        } else {
            return 0;
        }
    } else {
        if (a->average < b->average) {
            return 1;
        } else if (a->average > b->average) {
            return -1;
        } else {
            return 0;
        }
    }
}

/**
* (A/B/C/D) Reduce function for finding max/min average in sinfo
* linked list, bases result from current_query 
*
* @param head Pointer to head of sinfo linked list
* @return Pointer to sinfo node with max/min average
*/
static void *reduce_avg(sinfo *head) {
    sinfo *cursor = head->next, *result = head;
    char res;
    
    // Handle trivial cases
    if (head->next == NULL) {
        return head;
    }

    // Find query result
    while (cursor != NULL) {
        
        res = avgcmp(cursor, result);
        if (res > 0) {
            result = cursor;
        } 
        // Equal - pick alphabetical order first
        else if (res == 0) {
            if (strcmp(cursor->filename, result->filename) < 0) {
                result = cursor;
            }
        }
        cursor = cursor->next;
    }

    return result;
}

/**
* (E) Reduce function for finding country with the most users
*
* @param head Pointer to head of sinfo linked list
* @return Pointer to sinfo node with highest country user count
*/
static void *reduce_max_country(sinfo *head) {
    unsigned int ccount[CCOUNT_SIZE], maxind;
    memset(ccount, 0, CCOUNT_SIZE * sizeof(int));
    
    sinfo *cursor = head;
    while (cursor != NULL) {
        
        // Find index of max country user count
        maxind = 0;
        for (int i = 1; i < CCOUNT_SIZE; ++i) {
            // If i > maxind element OR i == maxind element and lex lesser
            if (cursor->einfo[i] > cursor->einfo[maxind]) {
                maxind = i;
            }
        }

        // Add max country to combined list
        ccount[maxind] += cursor->einfo[maxind];

        cursor = cursor->next;
    }

    // Find max country user count in combined list
    maxind = 0;
    for (int i = 1; i < CCOUNT_SIZE; ++i) {
        if (ccount[i] > ccount[maxind]) {
            maxind = i;
        }
    }

    head->filename[0] = (maxind / 26) + 'A';
    head->filename[1] = (maxind % 26) + 'A';
    head->filename[2] = '\0';
    head->average = ccount[maxind];

    return head;
}