package com.rashidmayes.clairvoyance;

import com.aerospike.client.Key;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rashidmayes.clairvoyance.model.InsertDocumentInfo;
import com.rashidmayes.clairvoyance.service.CRUDService;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.GridPane;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class InsertDocumentController {

    @FXML
    private GridPane rootPane;
    @FXML
    public Button btnInsert;
    @FXML
    public TextField txtNamespace;
    @FXML
    public TextField txtSet;
    @FXML
    private TextArea txtDocumentValue;

    private Tab thisTab;

    private ObjectMapper objectMapper = new ObjectMapper();
    private ObjectWriter objectWriter;

    private InsertDocumentInfo insertDocumentInfo;

    private boolean cancelled;

    private CRUDService crudService = CRUDService.crudServiceInstance;

    public InsertDocumentController() {
    }

    @FXML
    public void initialize() {
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectWriter = objectMapper.writerWithDefaultPrettyPrinter();

        rootPane.sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene == null) {
                cancelled = true;
            } else {
                insertDocumentInfo = (InsertDocumentInfo) rootPane.getUserData();
                thisTab = Browser.browserInstance.findTab(insertDocumentInfo.getId().toString());
                txtNamespace.setText(insertDocumentInfo.namespace);
                txtSet.setText(insertDocumentInfo.setName);
                txtDocumentValue.setText(insertDocumentInfo.value);
            }
        });
    }

    @FXML
    protected void handleBtnInsert(ActionEvent actionEvent) {
        ConcurrentHashMap<String, Object> concurrentHashMap;
        txtDocumentValue.getStyleClass().removeIf(s -> s.startsWith("color"));
        try {
            concurrentHashMap = objectMapper.readValue(txtDocumentValue.getText(), ConcurrentHashMap.class);
        } catch (Exception exception) {
            txtDocumentValue.getStyleClass().add("color-red");
            App.APP_LOGGER.severe("json parse error: " + exception.getMessage());
            exception.printStackTrace();
            return;
        }
        try {
            txtDocumentValue.setText(objectWriter.writeValueAsString(concurrentHashMap));
        } catch (Exception exception) {
            txtDocumentValue.getStyleClass().add("color-red");
            App.APP_LOGGER.severe("json writing error: " + exception.getMessage());
            exception.printStackTrace();
            return;
        }

        insertDocumentInfo.namespace = txtNamespace.getText();
        insertDocumentInfo.setName = txtSet.getText();
        insertDocumentInfo.value = txtDocumentValue.getText();

        try {
            crudService.save(txtNamespace.getText(), txtSet.getText(), concurrentHashMap,
                key -> {
                    App.APP_LOGGER.info(String.format("write success: %s %s %s", key.namespace, key.setName, key.userKey));
                    Browser.browserInstance.updateRecordRowOnSet(insertDocumentInfo.getSetId().toString(), key, concurrentHashMap);
                    Alert alert = new Alert(Alert.AlertType.INFORMATION, "Save success");
                    alert.showAndWait();
                }, exception -> {
                    App.APP_LOGGER.severe(String.format("write fail: %s %s %s: " + exception.toString(), insertDocumentInfo.namespace, insertDocumentInfo.setName, concurrentHashMap.getOrDefault("@user_key", concurrentHashMap.getOrDefault("id", ""))));
                    exception.printStackTrace();
                    Alert alert = new Alert(Alert.AlertType.ERROR, "Save failed:\n" + exception);
                    alert.showAndWait();
                }
            );
        } catch (Exception exception) {
            App.APP_LOGGER.severe(String.format("write fail: %s %s %s: " + exception.toString(), insertDocumentInfo.namespace, insertDocumentInfo.setName, concurrentHashMap.getOrDefault("@user_key", concurrentHashMap.getOrDefault("id", ""))));
            exception.printStackTrace();
            Alert alert = new Alert(Alert.AlertType.ERROR, "Save failed:\n" + exception);
            alert.showAndWait();
        }
    }

    /*
    private void createContextMenu() {
        dataTable.setRowFactory(tableView -> {
            TableRow<RecordRow> tableRow = new TableRow<RecordRow>();

            ContextMenu contextMenuOnExists = new ContextMenu();
            MenuItem menuItemInsertOnExists = new MenuItem("Insert");
            MenuItem menuItemDeleteOnExists = new MenuItem("Delete");
            contextMenuOnExists.getItems().addAll(menuItemInsertOnExists, menuItemDeleteOnExists);

            menuItemInsertOnExists.setOnAction(event -> {

            });
            menuItemDeleteOnExists.setOnAction(event -> {
                try {
                    crudService.delete(
                        tableRow.getItem().getKey().namespace,
                        tableRow.getItem().getKey().setName,
                        tableRow.getItem().getKey().userKey.toString()
                    );
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            });

            ContextMenu contextMenuOnNotExists = new ContextMenu();
            MenuItem menuItemInsertOnNotExists = new MenuItem("Insert");
            contextMenuOnNotExists.getItems().addAll(menuItemInsertOnNotExists);

            menuItemInsertOnNotExists.setOnAction(event -> {

            });

            tableRow.contextMenuProperty().bind(
                    Bindings.when(tableRow.emptyProperty())
                            .then(contextMenuOnNotExists)
                            .otherwise(contextMenuOnExists)
            );

            return tableRow;
        });
    }
    */

    @FXML public void handleTxtDocumentValueOnKeyTyped(KeyEvent keyEvent) {
        txtDocumentValue.setStyle("-fx-text-fill: black;");
    }
}
