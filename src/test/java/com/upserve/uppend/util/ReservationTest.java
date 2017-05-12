package com.upserve.uppend.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ReservationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkOut() {
        Reservation<Integer> reservation = new Reservation<>();

        assertTrue(reservation.checkout("1", 1));
        assertFalse(reservation.checkout("1", 1));
        assertTrue(reservation.checkout("2", 2));
        assertFalse(reservation.checkout("1", 1));
        assertFalse(reservation.checkout("2", 2));
    }

    @Test
    public void checkIn() {
        Reservation<Integer> reservation = new Reservation<>();

        reservation.checkin("1", 1);
        assertTrue(reservation.checkout("1", 1));
        assertFalse(reservation.checkout("1", 1));
        reservation.checkin("1", 1);
        assertTrue(reservation.checkout("1", 1));
    }

    @Test
    public void concurrency() {
        Reservation<Integer> reservation = new Reservation<>();

        AtomicInteger counter = new AtomicInteger();
        AtomicInteger truthCounter = new AtomicInteger();

        int[] closeOn = new int[]{300, 400, 500, 600};

        new Random().ints(1000, 0, 4).parallel().forEach(i -> {

            Integer count = counter.addAndGet(1);

            if (IntStream.of(closeOn).anyMatch(x -> x == count)) {
                reservation.checkin(String.format("%s", i), i);
            }

            if (reservation.checkout(String.format("%s", i), i)) {
                truthCounter.addAndGet(1);
            }

        });

        assertEquals(8, truthCounter.get());
    }
}
