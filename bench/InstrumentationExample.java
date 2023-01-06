import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InstrumentationExample {

    public static void printObjectSize(Object object) {
        System.out.println("Object type: " + object.getClass() +
          ", size: " + InstrumentationAgent.getObjectSize(object) + " bytes");
    }

    public static void main(String[] arguments) {

		class Person {
			String name;
			int age;
			long phone;
			boolean female;
			byte[] password = {1, 2, 3, 4};
		}

		Person p = new Person();

		int[] a0 = {};
		int[] a1 = {1};
		int[] a2 = {1, 2};
		int[] a3 = new int[100];

		String[] b0 = {};
		String[] b1 = {"1"};
		String[] b2 = {"1", "2"};
		String[] b3 = new String[100];

		String s0 = "";
		String s1 = "hello";

		List<Person> al0 = new ArrayList<>(0);
		List<Person> al1 = new ArrayList<>(1);
		al1.add(new Person());
		List<Person> al2 = new ArrayList<>(2);
		al2.add(new Person());
		al2.add(new Person());
		List<Person> al3 = new ArrayList<>(100);
		for (int i = 0; i < 100; i++) {
		    al3.add(new Person());
		}
	
		printObjectSize(p);

		printObjectSize(a0);
		printObjectSize(a1);
		printObjectSize(a2);
		printObjectSize(a3);

		printObjectSize(b0);
		printObjectSize(b1);
		printObjectSize(b2);
		printObjectSize(b3);

		printObjectSize(al0);
		printObjectSize(al1);
		printObjectSize(al2);
		printObjectSize(al3);

		printObjectSize(s0);
		printObjectSize(s1);
    }
}
