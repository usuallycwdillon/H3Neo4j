import org.graalvm.polyglot.*;
public class HelloPolyglotWorld {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello polyglot world Java!");
//        Context context = Context.create();
//        Context context = Context.newBuilder().allowAllAccess(true).build();
        Context context = Context.newBuilder().allowNativeAccess(true).build();
        context.eval("js", "print('Hello polyglot world JavaScript!');");
        context.eval("ruby", "puts 'Hello polyglot world Ruby!'");
        context.eval("python", "print('Hello polyglot world Python!');");
        context.eval("R", "print('Hello polyglot world R!');");
    }
}
