package p2;

import p2.storage.AllocationStrategy;
import p2.btrfs.BtrfsFile;
import p2.storage.FileSystem;
import p2.storage.Interval;
import p2.storage.StringEncoder;

import java.util.Arrays;

/**
 * Main entry point in executing the program.
 */
public class Main {

    /**
     * Main entry point in executing the program.
     *
     * @param args program arguments, currently ignored
     */
    public static void main(String[] args) {

        {
            int[] a = new int[]{1, 2, 3, 4, 5, 6, 7};
            int[] b = new int[a.length];
            System.arraycopy(a, 3, b, 0, 3);
            a = Arrays.copyOf(Arrays.copyOf(a, 3), 7);
            Interval[] aa = new Interval[]{new Interval(1,1), new Interval(2,1), new Interval(3,1), new Interval(4,1), new Interval(5,1), new Interval(6,1), new Interval(7,1)};
            System.out.println(Arrays.toString(aa));
            aa = Arrays.copyOf(Arrays.copyOf(aa, 3), 7);

            System.out.println(aa.length);
            System.out.println(Arrays.toString(a));
            System.out.println(Arrays.toString(b));
            System.out.println(Arrays.toString(aa));
        }

        {
            int degree = 3;
            int index = 1;
            int size = 3;
            int[] a = new int[]{1,2,3,0,0};
            int[] b = Arrays.copyOf(a, 5);
            for (int i = 3; i >= index; --i){
                int tmp = a[i];
                a[i] = a[i+1];
                a[i+1] = tmp;
            }
            System.arraycopy(b, index, b, index+1, size-index);
            //b[index] = 0;
            System.out.println(Arrays.toString(a)); // :D
            System.out.println(Arrays.toString(b));
        }

        String fileName = "example.txt";
        StringEncoder encoder = StringEncoder.INSTANCE;
        FileSystem fileSystem = new FileSystem(AllocationStrategy.NEXT_FIT, 200);

        BtrfsFile file = fileSystem.createFile(fileName, "Helo", encoder);

        System.out.println(fileSystem.readFile(fileName, encoder)); // Helo!

        fileSystem.insertIntoFile(fileName, 4, " World!", encoder);

        System.out.println(fileSystem.readFile(fileName, encoder)); // Helo World!

        fileSystem.insertIntoFile(fileName, 3, "l", encoder);

        System.out.println(fileSystem.readFile(fileName, encoder)); // Hello World!

        fileSystem.insertIntoFile(fileName, 6, "beautiful and very very very nice and wonderful and i dont know what else ", encoder);

        System.out.println(fileSystem.readFile(fileName, encoder, 0, file.getSize())); // Hello beautiful and very very very nice and wonderful and i dont know what else World!

        System.out.println(new String(file.readAll().getData()));

        fileSystem.removeFromFile(fileName, 6, 14);

        System.out.println(fileSystem.readFile(fileName, encoder)); // Hello very very very nice and wonderful and i dont know what else World

        fileSystem.removeFromFile(fileName, 6, 60);

        System.out.println(fileSystem.readFile(fileName, encoder)); // Hello World!

        // TODO: read does not pass all edge cases it seems, testing is needed

        final int length = file.readAll().getData().length;

        for (int i = 0; i <= length; ++i){
            System.out.println(fileSystem.readFile(fileName, encoder, 0, i));
        }
        for (int i = length; i >= 0; --i){
            System.out.println(fileSystem.readFile(fileName, encoder, 0, i));
        }

        for (int i = 0; i <= length ;++i){
            System.out.println(fileSystem.readFile(fileName, encoder, i, 2));
        }

        fileSystem.removeFromFile(fileName, 0, file.getSize());

        System.out.println(fileSystem.readFile(fileName, encoder)); // <empty>
    }
}
