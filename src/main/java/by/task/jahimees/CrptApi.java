package by.task.jahimees;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static by.task.jahimees.CrptApi.Utils.ObjectMapperConfigType;
import static by.task.jahimees.CrptApi.Utils.createMapper;

public class CrptApi {

    private final TimeUnit timeUnit;
    private final Semaphore semaphore;
    private final int blockDuration;
    //Внедрение через Property, либо передача параметром
    public static final String url = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    public CrptApi(TimeUnit timeUnit, int requestLimit, int blockDuration) {
        this.timeUnit = timeUnit;
        this.semaphore = new Semaphore(requestLimit);
        this.blockDuration = blockDuration;
    }

    public static void main(String[] args) {
        CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 2, 5);
        File file = new File("src/main/resources/document.json");

        Document document = Utils.deserialize(createMapper(ObjectMapperConfigType.CUSTOMIZED), file, Document.class);

        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                crptApi.createDocument(document, "mySign", url);
            }).start();
        }
    }

    /**
     * Создание документа через отправку запроса.
     * С помощью {@link Semaphore} достигается ограничение количества запросов в промежуток времени
     * @param document
     * @param sign
     * @param url
     */
    public void createDocument(Document document, String sign, String url) {

        HttpResponse<String> response;

        try {
            semaphore.acquire();
            System.out.println(Thread.currentThread().getId() + ": начал отправку запроса");

            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("Signature", Base64.getEncoder().encodeToString(sign.getBytes()))
                    .POST(HttpRequest.BodyPublishers.ofString
                            (createMapper(ObjectMapperConfigType.CUSTOMIZED).writeValueAsString(document.toString())))
                    .build();

            response = HttpClient.newHttpClient()
                    .send(httpRequest, HttpResponse.BodyHandlers.ofString());

            System.out.println(Thread.currentThread().getId() + ": запрос отправлен. засыпание...");
            try {
                Thread.sleep(timeUnit.toMillis(blockDuration));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println(Thread.currentThread().getId() + ": отпускает монитор");
            semaphore.release();
        }

        processResponse(response);
    }

    /**
     * Обработка ответа от сервера
     *
     * @param httpResponse ответ от сервера
     */
    public void processResponse(HttpResponse<String> httpResponse) {
        //Response processing
    }

    /**
     * Утилитный класс. Предоставляет доступ к созданию различных конфигураций {@link ObjectMapper}, а также
     * к десериализации объектов
     */
    protected static final class Utils {
        public enum ObjectMapperConfigType {
            DEFAULT, CUSTOMIZED
        }

        /**
         * Метод для создания {@link ObjectMapper} с возможностью выбора конфигурации
         *
         * @param configType тип конфигурации
         * @return объект objectMapper
         */
        public static ObjectMapper createMapper(ObjectMapperConfigType configType) {
            ObjectMapper mapper = new ObjectMapper();
            switch (configType) {
                case CUSTOMIZED -> customConfigMapper(mapper);
            }

            return mapper;
        }

        /**
         * Модифицированный {@link ObjectMapper}
         * <li>Выключена сереализация дат в {@link java.sql.Timestamp}</li>
         * <li>Включен модуль {@link JavaTimeModule}</li>
         *
         * @param mapper модифицируемый objectMapper
         */
        private static void customConfigMapper(ObjectMapper mapper) {
            mapper
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        }

        /**
         * Метод для десериализации объекта.
         * Сначала объект десериализуется стандартно, после определяется тип объекта и направляется
         * на глубокую десериализацию внутренних объектов
         *
         * @param mapper маппер
         * @param jsonFile файл, содержащий json-объект
         * @param type тип передаваемого объекта
         * @return десериализованный объект
         */
        private static <T> T deserialize(ObjectMapper mapper, File jsonFile, Class<T> type) {
            try {
                T obj = mapper.readValue(jsonFile, type);

                if (obj instanceof Document document) {
                    deepDeserializeDocument(document, jsonFile, mapper);
                }

                return obj;
            } catch (IOException ex) {
                throw new RuntimeException("deserialize: Обработка исключения");
            }
        }

        /**
         * Глубокая десериализация объекта {@link Document}
         *
         * @param obj частично десериализованный объект
         * @param jsonFile файл с объектом в виде json
         * @param mapper маппер
         */
        private static void deepDeserializeDocument(Document obj, File jsonFile, ObjectMapper mapper) {
            try {
                JsonNode node = mapper.readTree(jsonFile);

                //Description deserialize
                JsonNode descriptionNode = node.get("description");
                Description description = mapper.treeToValue(descriptionNode, Description.class);
                obj.setDescription(description);

                //Products deserialize
                JsonNode productsNode = node.get("products");
                obj.products = new ArrayList<>();

                if (productsNode.isArray()) {
                    for (JsonNode productNode : productsNode) {
                        obj.products.add(mapper.treeToValue(productNode, Product.class));
                    }
                }

            } catch (IOException ex) {
                throw new RuntimeException("deepDeserializeDocument: Обработка исключения");
            }
        }
    }


    public static class Document {
        private Description description;
        @JsonProperty("doc_id")
        private String docId;
        @JsonProperty("doc_status")
        private String docStatus;
        @JsonProperty("doc_type")
        private String docType;
        private boolean importRequest;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("participant_inn")
        private String participantInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("production_type")
        private String productionType;
        private List<Product> products;
        @JsonProperty("reg_date")
        private String regDate;
        @JsonProperty("reg_number")
        private String regNumber;

        public Description getDescription() {
            return description;
        }

        public void setDescription(Description description) {
            this.description = description;
        }

        public String getDocId() {
            return docId;
        }

        public void setDocId(String docId) {
            this.docId = docId;
        }

        public String getDocStatus() {
            return docStatus;
        }

        public void setDocStatus(String docStatus) {
            this.docStatus = docStatus;
        }

        public String getDocType() {
            return docType;
        }

        public void setDocType(String docType) {
            this.docType = docType;
        }

        public boolean isImportRequest() {
            return importRequest;
        }

        public void setImportRequest(boolean importRequest) {
            this.importRequest = importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public String getProductionDate() {
            return productionDate;
        }

        public void setProductionDate(String productionDate) {
            this.productionDate = productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public void setProductionType(String productionType) {
            this.productionType = productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public void setProducts(List<Product> products) {
            this.products = products;
        }

        public String getRegDate() {
            return regDate;
        }

        public void setRegDate(String regDate) {
            this.regDate = regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }

        public void setRegNumber(String regNumber) {
            this.regNumber = regNumber;
        }
    }

    public static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;
        @JsonProperty("certificate_document_date")
        private LocalDate certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private LocalDate productionDate;
        @JsonProperty("tnved_code")
        private String tnvedCode;
        @JsonProperty("uit_code")
        private String uitCode;
        @JsonProperty("uitu_code")
        private String uituCode;

        public String getCertificateDocument() {
            return certificateDocument;
        }

        public void setCertificateDocument(String certificateDocument) {
            this.certificateDocument = certificateDocument;
        }

        public LocalDate getCertificateDocumentDate() {
            return certificateDocumentDate;
        }

        public void setCertificateDocumentDate(LocalDate certificateDocumentDate) {
            this.certificateDocumentDate = certificateDocumentDate;
        }

        public String getCertificateDocumentNumber() {
            return certificateDocumentNumber;
        }

        public void setCertificateDocumentNumber(String certificateDocumentNumber) {
            this.certificateDocumentNumber = certificateDocumentNumber;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public void setProductionDate(LocalDate productionDate) {
            this.productionDate = productionDate;
        }

        public String getTnvedCode() {
            return tnvedCode;
        }

        public void setTnvedCode(String tnvedCode) {
            this.tnvedCode = tnvedCode;
        }

        public String getUitCode() {
            return uitCode;
        }

        public void setUitCode(String uitCode) {
            this.uitCode = uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }

        public void setUituCode(String uituCode) {
            this.uituCode = uituCode;
        }
    }

    public static class Description {
        private String participantInn;

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }
    }
}
