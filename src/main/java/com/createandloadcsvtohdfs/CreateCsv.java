package com.createandloadcsvtohdfs;
import au.com.anthonybruno.Gen;
import au.com.anthonybruno.generator.defaults.IntGenerator;
import com.github.javafaker.Faker;

public class CreateCsv {
    public static void main(String[] args) {
        createPeopleCsv(10);
    }
    public static void createPeopleCsv(int noOfFilesToGenerate)
    {
        for (int i = 1; i <= noOfFilesToGenerate; i++) {
            Faker faker = Faker.instance();
            Gen.start();
            Gen.start()
                    .addField("Name", () -> faker.name().firstName())
                    .addField("age", new IntGenerator(18, 80))
                    .addField("phone_number", ()-> faker.phoneNumber().cellPhone())
                    .addField("company", ()-> faker.company().name().replaceAll(",",""))
                    .addField("building_code", ()-> faker.address().buildingNumber())
                    .addField("address", ()-> faker.address().cityName())
                    .generate(30)
                    .asCsv()
                    .toFile("CSVFiles/People"+i+".csv");
        }

    }


}
