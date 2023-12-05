package exceptions;

@SuppressWarnings("serial")
public class QueryParseExceptions extends Exception {

    public QueryParseExceptions(String message) {
        super("Error parsing the query:" + message);
    }
}

