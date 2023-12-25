package hadoop.hive.udf;

public class TestUDF {
    private String str = new String();
    public String evaluate(String str){
        if (str == null){
            return null;
        }

        return str+"hello";
    }

    public static void main( String[] args ) {
        TestUDF str = new TestUDF();
        String evaluate = str.evaluate("0000");
        System.out.println(evaluate);
    }
}
