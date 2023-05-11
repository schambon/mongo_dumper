package org.schambon.dumper;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.RawBsonDocument;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

public class Dumper {

    private final static int BULKSIZE = 100;

    public static void main(String[] args) throws ParseException, IOException {
        var options = new Options();

        options.addOption(null, "uri", true, "Cluster URI");
        options.addOption("d", "database", true, "Database name");
        options.addOption("c", "collection", true, "Collection name");
        options.addOption("o", "out", true, "Output file");
        options.addOption(null, "to-uri", true, "Destination URI");
        options.addOption(null, "to-db", true, "Destination DB (default same as --database)");
        options.addOption(null, "to-coll", true, "Destination Coll (default same as --collection)");
        options.addOption(null, "clear", false, "Clear destination");


        var parser = new DefaultParser();
        var line = parser.parse(options, args);

        var c = new Config();
        c.uri = line.getOptionValue("uri");
        c.database = line.getOptionValue("database");
        c.collection = line.getOptionValue("collection");
        c.output = line.getOptionValue("out");
        c.toUri = line.getOptionValue("to-uri");
        c.toDb = line.getOptionValue("to-db");
        c.toColl = line.getOptionValue("to-coll");
        if (c.toDb == null) c.toDb = c.database;
        if (c.toColl == null) c.toColl = c.collection;
        c.clear = line.hasOption("clear");


        if ((c.uri == null || c.database == null || c.collection == null) || (c.output == null && c.toUri == null)) {
            System.err.println("Usage: Dumper --uri <uri> --database <db> --collection <coll> (--out <outfile> | --to-uri <uri> [--to-db <db>] [--to-coll coll]");
            System.exit(1);
        }

        new Dumper(c).start();
    }

    private Dumper(Config c) {
        this.config = c;
    }

    Config config;

    public void start() throws IOException {
        var client = MongoClients.create(config.uri);
        var coll = client.getDatabase(config.database).getCollection(config.collection, RawBsonDocument.class).withReadPreference(ReadPreference.secondaryPreferred());

        var dest = new Destination(config);

        var i = 0;
        for (var rawBson : coll.find()) {

            dest.write(rawBson);
            if (++i % BULKSIZE == 0) {
                dest.flush();
                System.out.print(".");
            }
        }

        dest.flush();
        dest.close();

        System.out.println(String.format(".\nCopied %d documents", i));
    }

    private static class Destination {
        MongoCollection<RawBsonDocument> collection = null;
        BufferedOutputStream outputStream = null;

        boolean isFile = true;

        List<RawBsonDocument> buffer = new ArrayList<>(BULKSIZE);

        Destination(Config c) throws FileNotFoundException {
            if (c.output != null) {
                isFile = true;
                outputStream = new BufferedOutputStream(new FileOutputStream(c.output), 4096*1024);
            } else {
                isFile = false;
                collection = MongoClients.create(c.toUri).getDatabase(c.toDb).getCollection(c.toColl, RawBsonDocument.class);
                if (c.clear) {
                    collection.drop();
                }
            }
        }

        void write(RawBsonDocument doc) throws IOException {
            if (isFile) {
                outputStream.write(doc.getByteBuffer().array(), 0, doc.getByteBuffer().limit());
            } else {
                buffer.add(doc);
            }
        }

        void flush() throws IOException {
            if (isFile) {
                outputStream.flush();
            } else {
                collection.insertMany(buffer, new InsertManyOptions().ordered(false));
                buffer.clear();
            }
        }

        void close() throws IOException {
            if (isFile) {
                outputStream.close();
            }
        }
    }

    private static class Config {
        public boolean clear;
        public String toColl;
        public String toDb;
        public String toUri;
        public String uri;
        public String database;
        public String collection;
        public String output;
    }
}

