package com.upserve.uppend.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reservation system for instances
 * Used with AppendOnlyStores to enforce that only one instance of a particular store exists.
 */
public class Reservation<T>{

    private final Map<String, T> reservations = new ConcurrentHashMap<>();

    /**
     * Checkout a reservation for this instance
     *
     * @param instance is the object requesting a reservation
     * @return true if you got the reservation. False if it is already reserved.
     */
    public boolean checkout(String key, T instance){
        AtomicBoolean result = new AtomicBoolean(false);
        reservations.computeIfAbsent(key, k -> {
            result.set(true);
            return instance;
        });
        return result.get();
    }

    /**
     * Checkin a reservation when the instance is done with it (during close)
     *
     * @param instance is the object relinquishing a reservation
     */
    public void checkin(String key, T instance){
        reservations.remove(key, instance);
    }
}
