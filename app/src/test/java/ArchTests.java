import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@interface Service {
}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@interface LegacyBridge {
}

public class ArchTests {
@Test
public void serviceClasses_ShouldResideInAppropriatePackage() {
    var importedClasses = new ClassFileImporter().importPackages("com.myapp");

    var rule =
            classes().that()
                    .areAnnotatedWith(Service.class).or().haveNameMatching(".*Service")
            .should()
                    .resideInAPackage("..service..").orShould().beAnnotatedWith(LegacyBridge.class);

    rule.check(importedClasses);
}



}