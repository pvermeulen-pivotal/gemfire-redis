package com.vmware.gemfire.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.resps.GeoRadiusResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RedisClient {

    private JedisPool pool;
    private BufferedReader reader;

    public RedisClient(String host, Integer port) {
        pool = new JedisPool(host, port);
        createReader();
    }

    public RedisClient() {
        pool = new JedisPool("localhost", 6379);
        createReader();
    }

    private void createReader() {
        reader = new BufferedReader(
                new InputStreamReader(System.in));
    }

    private void showClusterNodes() {
        clusterNodes();
    }

    private void readLine() {
        try {
            reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runStringCommands() {
        System.out.print("Start Redis String Commands -->");
        readLine();
        createString();
        System.out.print("\nresults: \n");
        printString();
        System.out.print("continue....\n");
        readLine();
    }

    private void runSetCommands() {
        System.out.print("Start Redis Set Commands -->");
        readLine();
        createSet();
        System.out.print("\nresults: \n");
        printSet();
        System.out.print("continue....\n");
        readLine();
    }

    private void runHashCommands() {
        System.out.print("Start Redis Hash Commands -->");
        readLine();
        createHash();
        System.out.print("\nresults: \n");
        printHash();
        System.out.print("continue....\n");
        readLine();
    }

    private void runListCommands() {
        System.out.print("Start Redis List Commands -->");
        readLine();
        createList();
        System.out.print("\nresults: \n");
        printList();
        System.out.print("continue....\n");
        readLine();
    }

    private void runSortedListCommands() {
        System.out.print("Start Redis Sorted List Commands -->");
        readLine();
        createSortedList();
        System.out.print("\nresults: \n");
        printSortedList();
        System.out.print("continue....\n");
        readLine();
    }

    private void runGeoSpacialCommands() {
        System.out.print("Start Redis Geo Spacial Commands -->");
        readLine();
        createGeoSpacial();
        System.out.print("\nresults: \n");
        printGeoSpacial();
        System.out.print("continue....\n");
        readLine();
    }

    private void runHyperLogCommands() {
        System.out.print("Start Redis Hyper Log Commands -->");
        readLine();
        createHyperLog();
        System.out.print("\nresults: \n");
        printHyperlog();
        System.out.print("continue....\n");
        readLine();
    }

    private void close() {
        try {
            pool.clear();
            pool.close();
            reader.close();
        } catch (Exception ex) {
            log.error("Error  closing resources");
        }
    }

    private void createGeoSpacial() {
        Jedis jedis = pool.getResource();
        jedis.geoadd("kentucky", 38.328732d, -85.764771d, "louisville");
        jedis.geoadd("kentucky", 38.047989d, -84.501640d, "lexington");
        pool.returnResource(jedis);
    }

    private void printGeoSpacial() {
        Jedis jedis = pool.getResource();
        Double dist = jedis.geodist("kentucky", "louisville", "lexington", GeoUnit.MI);
        System.out.println("Distance between loouisville and lexington: " + dist + " miles" );
        GeoCoordinate coor = new GeoCoordinate(38.1d, -85.0d);
        List<GeoRadiusResponse> resp = jedis.geosearch("kentucky", coor, 5, GeoUnit.MI);
        resp.forEach(a -> {
            System.out.println("member: " + a.getMemberByString() + " distance: " + a.getDistance());
        });
        pool.returnResource(jedis);
    }

    private void createHyperLog() {
        Jedis jedis = pool.getResource();
        jedis.pfadd("hyperlog", "members", "12");
        jedis.pfadd("hyperlog", "members", "25");
        pool.returnResource(jedis);
    }

    private void printHyperlog() {
        Jedis jedis = pool.getResource();
        System.out.println("hyperlog count: " + jedis.pfcount("hyperlog"));
        pool.returnResource(jedis);
    }

    private void clusterNodes() {
        Jedis jedis = pool.getResource();
        String nodes = jedis.clusterNodes();
        System.out.println("cluster nodes: " + nodes);
        pool.returnResource(jedis);
    }

    private void createString() {
        Jedis jedis = pool.getResource();
        jedis.set("Name", "George");
        pool.returnResource(jedis);
    }

    private void printString() {
        Jedis jedis = pool.getResource();
        System.out.println("String: " + jedis.get("Name"));
        pool.returnResource(jedis);
    }

    private void createSet() {
        Jedis jedis = pool.getResource();
        jedis.sadd("members", "server1", "server2", "server3");
        pool.returnResource(jedis);
    }

    private void printSet() {
        Jedis jedis = pool.getResource();
        Set<String> members = jedis.smembers("members");
        members.forEach(mem -> {
            System.out.println("Set: " + mem);
        });
        pool.returnResource(jedis);
    }

    private void createHash() {
        String key = "member";
        Map<String, String> map = new HashMap<>();
        map.put("name", "mem1");
        map.put("domain", "www.mem1.com");
        map.put("description", "Insert mem1");
        Jedis jedis = pool.getResource();
        jedis.hmset(key, map);
        pool.returnResource(jedis);
    }

    private void printHash() {
        Jedis jedis = pool.getResource();
        Map<String, String> retrieveMap = jedis.hgetAll("member");
        retrieveMap.forEach((k, v) -> {
            System.out.println("HashMap: " + k + " " + retrieveMap.get(k));
        });
        pool.returnResource(jedis);
    }

    private void createList() {
        Jedis jedis = pool.getResource();

        jedis.lpush("Members", "server1", "server2", "server3");
        pool.returnResource(jedis);
    }

    private void printList() {
        Jedis jedis = pool.getResource();
        System.out.println("List Pop Last Entry: " + jedis.lpop("Members"));
        pool.returnResource(jedis);
    }

    private void createSortedList() {
        Jedis jedis = pool.getResource();
        jedis.zadd("Score", 21.5d, "member1");
        jedis.zadd("Score", 18.8d, "member2");
        jedis.zadd("Score", 19.2d, "member3");
        pool.returnResource(jedis);
    }

    private void printSortedList() {
        Jedis jedis = pool.getResource();
        System.out.println("SortedList: " + jedis.zscore("Score", "member1"));
        pool.returnResource(jedis);
    }

    public static void main(String[] args) {
        RedisClient client;
        if (args != null && args.length == 2) {
            client = new RedisClient(args[0], Integer.parseInt(args[1]));
        } else {
            log.info("Starting Redis client with defaults");
            client = new RedisClient();
        }

        client.showClusterNodes();
        client.runStringCommands();
        client.runSetCommands();
        client.runHashCommands();
        client.runListCommands();
        client.runSortedListCommands();

        client.close();
    }
}
