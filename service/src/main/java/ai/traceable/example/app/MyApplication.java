package ai.traceable.example.app;

import ai.traceable.example.impl.MyServiceImplementation;

class MyApplication {

    public static void main(String[] args) {
        System.out.println("Started application. GetText: " + new MyServiceImplementation().getText());
    }
}