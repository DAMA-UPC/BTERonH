package ldbc.snb.bteronh.structures;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by aprat on 17/08/16.
 */
public class Arguments {
    public static class Property {
        private String property;
        private String value;

        public Property(String property, String value) {
            this.property = property;
            this.value = value;
        }

        public String getProperty() {
            return property;
        }

        public String getValue() {
            return value;
        }
    }

    public static class PropertyConverter implements IStringConverter<Property> {
        @Override
        public Property convert(String s) {
            int colon = s.indexOf(":");
            if(colon == -1 ) {
                System.err.println("Incorrect property format. Missing \":\"");
                System.exit(-1);
            }
            return new Property(s.substring(0,colon), s.substring(colon+1,s.length()));
        }
    }

    @Parameter(names = "-p", description = "Property", converter=PropertyConverter.class)
    private List<Property> properties = new ArrayList<Property>();

    public List<Property> getProperties() {
        return properties;
    }
}
