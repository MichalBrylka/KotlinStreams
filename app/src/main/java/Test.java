public class Test {
}


//interface TextSource {
//    String getText();
//}
//
////klasa dostarczająca zawsze ten sam tekst
//class StringSource implements TextSource {
//    //TODO dodaj odpowiednie pola i konstruktory, może warto użyć biblioteki Lombok ?
//    @Override
//    public String getText() {
//        throw new Exception("TODO");
//    }
//}
//
////klasa dostarczająca losowy tekst
//class RandomTextSource implements TextSource {
//    //może warto mieć tu jakieś pole typu Random ?
//    @Override
//    public String getText() {
//        throw new Exception("TODO");
//    }
//}
//
//class FileSource implements TextSource {
//    private final String fileName; //zainicjalizuj to jakoś
//
//    @Override
//    public String getText() {
//        throw new Exception("TODO"); //dodaj czytanie z pliku
//    }
//}
//
//class WebResourceSource implements TextSource {
//    private final String url; //zainicjalizuj to jakoś
//
//    @Override
//    public String getText() {
//        throw new Exception("TODO"); //dodaj pobieranie z adresu url np. https://loremipsum.de/downloads/version1.txt
//    }
//}
//
//interface WordCounter {
//    Integer countWords();
//}
//
//class SimpleWordCounter implements WordCounter {
//    private final TextSource source;
//
//    SimpleWordCounter(TextSource source) {
//        this.source = source;
//    }
//
//    @Override
//    public Integer countWords() {
//        var text = source.getText();
//
//        return null; //dodaj tu swoją implementację obliczającą słowa w 'text'
//    }
//}
//
////TODO dodaj klasy:
////1. EfficientWordCounter - klasa przechodząca przez łańcuch znaków przy pomocy pętli for
//
//
////2. CustomWordCounter - klasa pozwalająca jakoś wpływać na to co rozumiemy jako biały znak / rozdzielacz słów. Może warto to zrobić przy pomocy interfejsów funkcyjnych:
////https://www.baeldung.com/java-8-functional-interfaces
////?
//
////3. UniqueWordsCounter - klasa zliczająca tylko unikalne słowa np. "Ala ala ala" - w zależności na ustawienia (rozróżnianie wielkości liter) zwróci tu 1 lub 2 ale nie 3