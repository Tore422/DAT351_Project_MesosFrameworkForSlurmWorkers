package main;

public class CountToTen {
    public static void main(String[] args) {
        final int TEN = 10;
        int counter = 0;
        while (counter < TEN) {
            System.out.print(++counter + " ");
        }
    }
}
