//oto system w którym obejmujemy abstrakcją dowolne wyrażenia matematyczne. Nie zastanawiaj się jak się je parsuje, tylko załóż, że dane Ci będzie jakaś instancja IExpression

interface IExpression {
}

record Literal(float value) implements IExpression {
}

enum Operator {
    PLUS, MINUS, TIMES, DIVIDE, POWER
}

abstract class BinaryExpression implements IExpression {
    private final IExpression left;
    private final IExpression right;

    public BinaryExpression(IExpression left, IExpression right) {
        this.left = left;
        this.right = right;
    }

    public IExpression getLeft() {
        return left;
    }

    public IExpression getRight() {
        return right;
    }

    abstract Operator getOperator();
}

class Addition extends BinaryExpression {
    public Addition(IExpression left, IExpression right) {
        super(left, right);
    }

    @Override
    public Operator getOperator() {
        return Operator.PLUS;
    }
}

class Multiplication extends BinaryExpression {
    public Multiplication(IExpression left, IExpression right) {
        super(left, right);
    }

    @Override
    public Operator getOperator() {
        return Operator.TIMES;
    }
}

//TODO dodaj pozostałe operatory, opcjonalnie także inne typu wyrażeń np. wywołanie funkcji

class Program {
    public static void Main() {
        //1+2*3
        var expression = new Addition(new Literal(1), new Multiplication(new Literal(2), new Literal(3)));
        // TODO tu dodasz jakieś operacje na podstawie poniższych zadań
    }
}


//TODO Jest tu 1 wzorzec projektowy który bardziej niż cokolwiek innego tutaj pasuje do problemu. Znajdź go albo zaimplementuj zadanie po swojemu
//1. napisz funkcję która zwróci nam zapis wyrażenia (String) w formie "literał OPERATOR literał" np. dla powyższego to będzie "1 + 2 * 3". Jeśli dodasz swoje operacje np. wywołanie funkcji to zaproponuj jak to się będzie formatowało
//2. napisz funkcję która zwróci nam zapis wyrażenia (String) w RPN: https://en.wikipedia.org/wiki/Reverse_Polish_notation
//3. napisz funkcję która zwróci nam zapis wyrażenia (String) w MathMl: https://en.wikipedia.org/wiki/MathML
//   Możesz założyć że dzielenie ładnie się zapisuje ułamkami (mfrac). Po prostu ma być ładnie ;-)
//4. (dodatkowe) napisz funkcję która zwróci nam zapis wyrażenia w formie obrazu np. może to być bitmapa, dokument PDF (można użyć bibliotek iText lub PdfBox), albo dowolny inny format
