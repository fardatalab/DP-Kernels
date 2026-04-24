#include "socket.hpp"
#include "DuckDB-API/duckdb.h"
// #include <nlohmann/json.hpp>
#include <fstream>

// using json = nlohmann::json;

#define CONNECTION_FROM "0.0.0.0"
#define DBFILE_PATH "/home/kaiwen/dbfiles/sf1.db"
#define IMDB_PATH "/home/kaiwen/dbfiles/sf1"

ssize_t process_request(duckdb_connection conn, int client_sock);
int setup_db_connection(duckdb_database* db, duckdb_connection* conn, const char* dbfile_path, const char* db_dir);
void teardown_db_connection(duckdb_database* db, duckdb_connection* conn);
void parse_stats(const char* src);

int main(int argc, char** argv) {
    ssize_t nbytes_sent;
    uint64_t checksum = 0xdeadbeef;

    int listen_fd = server_init(PORT, CONNECTION_FROM);
    std::cout << "[server] listening with fd " << listen_fd << " on port " << PORT << '\n'; 
    int client_sock = accept_connection(listen_fd);
    std::cout << "[server] accepted connection from client with fd " << client_sock << '\n';

    duckdb_database db;
    duckdb_connection conn;
    if (setup_db_connection(&db, &conn, NULL, IMDB_PATH) == -1) {
        close(client_sock);
        exit(1);
    }
    
    // load db into memory and tell the client it's done
    checksum = htonl(checksum);
    nbytes_sent = send(client_sock, &checksum, sizeof(checksum), 0);
    if (nbytes_sent < 0) {
        std::cout << "[server] error: failed to send checksum bytes\n";
        teardown_db_connection(&db, &conn);
        close(listen_fd);
        exit(1);
    }

    // process query and send the result back
    nbytes_sent = process_request(conn, client_sock);
    if (nbytes_sent < 0) {
        teardown_db_connection(&db, &conn);
        close(listen_fd);
        exit(1);
    }

    teardown_db_connection(&db, &conn);
    close(listen_fd);
    
    return 0;
}

ssize_t process_request(duckdb_connection conn, int client_sock) {
    char query[BUF_SIZE];
    ssize_t nbytes_read, nbytes_sent;
    nbytes_read = recv(client_sock, query, BUF_SIZE, 0);
    if (nbytes_read < 0) {
        return -1;
    }
    std::cout << "[server] received " << nbytes_read << " bytes from client with fd " << client_sock << '\n';;

    // connect to duckdb and run query
    duckdb_result result;
    // execute query
    auto start = chrono::high_resolution_clock::now();
    if (duckdb_query(conn, query, &result) == DuckDBError) {
        std::cout << "[server] error: failed to process received query\n";
        return -1;
    }
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    std::cout << "[server] query execution duration: " << elapsed.count() << " sec\n";

    // retrive result from row 0, column 0 of the returned table
    uint64_t count = 0;
    if (duckdb_row_count(&result) > 0) {
        count = duckdb_value_uint64(&result, 0, 0);
    }
    
    // reply to client with count
    nbytes_sent = send(client_sock, &count, sizeof(uint64_t), 0);
    if (nbytes_sent < 0) {
        std::cout << "[server] error: failed to send query outcome to client\n";
        return -1;
    }
    duckdb_destroy_result(&result);
    std::cout << "[server] replied with " << nbytes_sent << " bytes\n";
    return nbytes_sent;
}

int setup_db_connection(duckdb_database* db, duckdb_connection* conn, const char* dbfile_path, const char* db_dir) {
    if (!(!dbfile_path || db_dir)) {
        std::cout << "[server] error: can't set up db connection\n";
        return -1;
    }
    if (duckdb_open(dbfile_path, db) == DuckDBError) {
        std::cout << "[server] error: can't set up db connection\n";
        return -1;
    }
    if (duckdb_connect(*db, conn) == DuckDBError) {
        std::cout << "[server] error: can't set up db connection\n";
        return -1;
    }
    if (!dbfile_path) {
        // import database
        char import_query[1024] = {0};
        char import[32] = "IMPORT DATABASE '";
        memcpy(import_query, import, strlen(import));
        memcpy(import_query + strlen(import), db_dir, strlen(db_dir));
        memcpy(import_query + strlen(import) + strlen(db_dir), "';", 2);
        assert(duckdb_query(*conn, import_query, NULL) != DuckDBError);
    }
    return 0;
}

void teardown_db_connection(duckdb_database* db, duckdb_connection* conn) {
    duckdb_disconnect(conn);
    duckdb_close(db);
}
