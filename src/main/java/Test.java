import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Properties;

public class Test {

    public static void main(String[] args ) throws Exception {
        Properties properties = new Properties();
        FileInputStream in = new FileInputStream("C:\\Dev\\BigData\\city.en.properties");
        properties.load(in);
        System.out.println(properties);
        System.out.println(properties.getProperty("217"));


        URI uri = new URI ("/user/maria_dev/04_bid/city.en.properties");

        if (uri.getPath().contains("city.en.properties")) {
            System.out.println("!!!!!");
        }

        System.out.println ("Authority = " +
                uri.getAuthority ());

        System.out.println ("Fragment = " +
                uri.getFragment ());

        System.out.println ("Host = " +
                uri.getHost ());

        System.out.println ("Path = " +
                uri.getPath ());

        System.out.println ("Port = " +
                uri.getPort ());

        System.out.println ("Query = " +
                uri.getQuery ());

        System.out.println ("Scheme = " +
                uri.getScheme ());

        System.out.println ("Scheme-specific part = " +
                uri.getSchemeSpecificPart ());

        System.out.println ("User Info = " +
                uri.getUserInfo ());

        System.out.println ("URI is absolute: " +
                uri.isAbsolute ());

        System.out.println ("URI is opaque: " +
                uri.isOpaque ());
    }
}
