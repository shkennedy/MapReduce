#include "lott.h"
#include "part4.h"

sem_t mut_pdcr, mut_buf;
int nproducers;
sinfo *buf_head;

int part4(size_t nthreads) {
    // Handle bad calls
    if (nthreads < 1) {
        return -1;
    }

    // Initialize semaphores
    sem_init(&mut_pdcr, 0, 1), sem_init(&mut_buf, 0, 1);

    // Create linked list of sinfo nodes, nfiles long
    sinfo *head = NULL, *cursor;
    int nfiles = make_files_list(&head);
    cursor = head;

    // Spawn and name reduce thread
    pthread_t t_reduce;
    char threadname[THREADNAME_SIZE] = {'r','e','d','u','c','e','\0'};
    sinfo result;
    memset(&result, 0, sizeof(sinfo));
    if (current_query == E) {
        result.einfo = calloc(CCOUNT_SIZE, sizeof(int));
    }
    pthread_create(&t_reduce, NULL, reduce, &result);
    pthread_setname_np(t_reduce, threadname);

    // Divide sinfo list equally between map threads
    pthread_t t_readers[nthreads];
    margs args[nthreads];
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

    // Cancel reduce thread since all map threads have been joined
    pthread_cancel(t_reduce);
    pthread_join(t_reduce, NULL);

    // Restore resources
    sinfo *prev;
    cursor = buf_head;
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

// Semaphore locking storage access for global buffer list
static void s_storeinfo(sinfo *info) {
    // Wait to change nproducers
    sem_wait(&mut_pdcr);
    if (++nproducers == 1) {
        // Only producer here - wait for access lock
        sem_wait(&mut_buf);
    }
    // Let others change nproducers
    sem_post(&mut_pdcr);

    // Store results in global buffer list
    info->next = buf_head;
    buf_head = info;

    sem_wait(&mut_pdcr);
    if (--nproducers == 0) {
        // Only producer here - free access lock
        sem_post(&mut_buf);
    }
    sem_post(&mut_pdcr);
}

// Semaphore locking consumer access for global buffer list
static int s_consumeinfo(sinfo **info) {
    int r = 0;
    // Wait for all producers to free access lock
    usleep(1);
    sem_wait(&mut_buf);

    // Consume from global buffer list
    if ((*info = buf_head) != NULL) {
        buf_head = buf_head->next;
        r = 1;
    } else {
        r = 0;
    }

    // Free access lock
    sem_post(&mut_buf);
    return r;
}

/**
* Map controller, calls map function for current query,
* Acts as start routine for created threads 
*
* @param v Pointer to margs container for map arguments
*/
static void* map(void* v) {
    margs *args = v;
    sinfo *info = args->head, *next;
    
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
        // Open file
        strcpy(filepath + 7, info->filename);
        info->file = fopen(filepath, "r");
        if (info->file == NULL) {
            exit(EXIT_FAILURE);
        }

        // Call map for query
        (*f_map)(info);

        // Store file info to global buffer list
        next = info->next;
        s_storeinfo(info);

        // Close file
        fclose(info->file);
        info = next;
    }
    
    pthread_exit(NULL);
    return NULL;
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

    // Write average duration 
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

    // Find max country count with lexicographical tie breaking
    for (int i = 0; i < CCOUNT_SIZE; ++i) {
        if (info->einfo[i] > info->einfo[ind]) {
            ind = i;
        }
    } 
    info->average = ind;
}

/**
* Cancel routine for the reduce thread, prints results of query
*
* @param v Pointer to sinfo containing results
*/
static void reduce_cancel(void *v) {
    sinfo *result = v;
    if (current_query == E) {
        int max = 0;
        for (int i = 1; i < CCOUNT_SIZE; ++i) {
            if (result->einfo[i] > result->einfo[max]) {
                max = i;
            }
        }

        result->filename[0] = (max / 26) + 'A';
        result->filename[1] = (max % 26) + 'A';
        result->filename[2] = '\0';
        result->average = result->einfo[max];
    }

    printf(
        "Part: %s\n"
        "Query: %s\n"
        "Result: %lf, %s\n",
        PART_STRINGS[current_part], QUERY_STRINGS[current_query],
        result->average, result->filename);
    fflush(NULL);
}

/**
* Reduce controller, calls reduce function for current query
* 
* @param v Pointer to head of sinfo linked list
* @return Pointer to sinfo containing result
*/
static void *reduce(void *v) {
    pthread_cleanup_push(&reduce_cancel, v);

    // Find reduce for current query
    void (*f_reduce)(sinfo*);
    if (current_query == E) {
        f_reduce = &reduce_max_country;
    } else {
        f_reduce = &reduce_avg;
    }

    // Find query result
    sinfo *result = v;
    (*f_reduce)(result);

     pthread_cleanup_pop(1);
     return NULL;
}

// Helper for reduce_avg
// Returns comparison based on current_query
static char avgcmp(double a, double b) {
    if (current_query == A || current_query == C) {
        if (a > b) {
            return 1;
        } else if (a < b) {
            return -1;
        } else {
            return 0;
        }
    } else {
        if (a < b) {
            return 1;
        } else if (a > b) {
            return -1;
        } else {
            return 0;
        }
    }
}

/**
* (A/B/C/D) Reduce function for finding max/min average in mapred.tmp, 
* bases result from current_query 
*
* @param result Pointer of sinfo to store result in
*/
static void reduce_avg(sinfo *result) {
    sinfo *cursor = NULL;
    char res;
    if (current_query == B || current_query == D) {
        result->average = 0x7FFFFFFF;
    }

    // Until cancelled
    while (1) {
        // Consume available info from global buffer list
        while (s_consumeinfo(&cursor)) {
            
            // Block canceling since there is an entry
            pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

            // Compare with current best
            res = avgcmp(cursor->average, result->average);
            if (res > 0) {
                result->average = cursor->average;
                strcpy(result->filename, cursor->filename);
            }
            // Equal - pick alphabetical order first
            else if (res == 0) {
                if (strcmp(cursor->filename, result->filename) < 0) {
                    result->average = cursor->average;
                    strcpy(result->filename, cursor->filename);
                }
            }
        }
        // Ran out of entries - enable canceling
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }
}

/**
* (E) Reduce function for finding country with the most users
*
* @param result Pointer of sinfo to store result in
*/
static void reduce_max_country(sinfo *result) {
    sinfo *cursor = NULL;
    
    // Until cancelled
    while (1) {
        
        // Read line of file and add count to index code 
        while (s_consumeinfo(&cursor)) {

            // Block canceling since there is an entry
            pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
  
            
            // Add count to country code
            result->einfo[(int)cursor->average] += 
            cursor->einfo[(int)cursor->average]; 
        }
        // Enable canceling 
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }
}
