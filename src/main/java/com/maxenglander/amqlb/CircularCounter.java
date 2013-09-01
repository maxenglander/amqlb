package com.maxenglander.amqlb;

/**
 *
 * @author maxenglander
 */
public class CircularCounter {
    private final int max;
    private int value;
    
    public CircularCounter(int start, int max) {
        if(start > max) {
            throw new IllegalArgumentException("start (" + start + ") cannot be greater than max (" + max + ")");
        }        
        this.max = max;
        this.value = start;
    }

    public synchronized int getAndIncrement() {
        final int previousValue = value;
        value++;
        if(value > max) {
            value = 0;
        }
        return previousValue;
    }
}
