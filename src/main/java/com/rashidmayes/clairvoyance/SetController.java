package com.rashidmayes.clairvoyance;

import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rashidmayes.clairvoyance.service.CRUDService;
import com.rashidmayes.clairvoyance.util.FileUtil;
import gnu.crypto.util.Base64;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.GridPane;
import javafx.util.Callback;
import javafx.util.Pair;
import jdk.nashorn.internal.runtime.regexp.joni.Regex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class SetController implements ScanCallback {

    public static final int DIGEST_LEN = 20;

    @FXML
    private GridPane rootPane;
    @FXML
    private ListView<Integer> pages;
    @FXML
    private TextArea recordDetails;
    @FXML
    private TableView<RecordRow> dataTable;
    @FXML
    private TabPane tabs;
    @FXML
    public Button btnRefresh;
    @FXML
    public TextField txtSearch;
    @FXML
    public CheckBox chkRegex;

    private ObjectMapper mObjectMapper = new ObjectMapper();
    private ObjectWriter mObjectWriter;

    private SetInfo mSetInfo;
    private Thread mScanThread;
    private int mRecordCount = 1;
    private int mPageCount = 0;
    private Thread mCurrentLoader;
    private Set<String> mColumns = new HashSet<String>();
    private Set<String> mKnownColumns = new HashSet<String>();


    private int mMaxBufferSize = 500;
    private int mMaxKeyBufferSize = 50000;
    private int mMaxPageZeroSize = 200000;

    private ArrayList<RecordRow> mRowBuffer = new ArrayList<RecordRow>(mMaxBufferSize);
    private ArrayList<byte[]> mKeyBuffer = new ArrayList<byte[]>(mMaxKeyBufferSize);

    private File mRootDir;
    private boolean cancelled;

    private Tab thisTab;
    private CRUDService crudService = CRUDService.crudServiceInstance;

    public SetController() {
    }

    @FXML
    public void initialize() {

        mObjectMapper.setSerializationInclusion(Include.NON_NULL);
        mObjectWriter = mObjectMapper.writerWithDefaultPrettyPrinter();

        rootPane.sceneProperty().addListener((obs, oldScene, newScene) -> {

            if (newScene == null) {
                //stop scan
                //clean files
                cancelled = true;
                if (mRootDir != null) {
                    App.EXECUTOR.execute(new Runnable() {
                        public void run() {
                            if (FileUtils.deleteQuietly(mRootDir)) {
                                App.APP_LOGGER.info("Removed " + mRootDir.getAbsolutePath());
                            }
                        }
                    });
                }
            } else {
                startScan();
            }
        });

        // update recordDetails
        dataTable.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue != null) {
                try {
                    Map<String, Object> newBins = newValue.getRecord().bins;
                    /*if (newBins.containsKey("@user_key")) {
                        String newId = newBins.get("@user_key").toString();
                        newBins.remove("@user_key");
                        newBins.put("id", newId);
                        mSetInfo.valueDumped = mObjectWriter.writeValueAsString(newBins);
                    } else {
                        mSetInfo.valueDumped = "";
                    }*/
                    mSetInfo.valueDumped = mObjectWriter.writeValueAsString(newBins);
                    recordDetails.setText(mObjectWriter.writeValueAsString(newValue));
                } catch (Exception e) {
                    App.APP_LOGGER.warning(e.getMessage());
                }
            }
        });

        pages.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue != null) {
                loadPage(newValue);
            }
        });
    }

    public void startScan() {
        // initialize
        if (mScanThread != null) {
            cancelled = true;
            mScanThread = null;
        }
        cancelled = false;

        mRecordCount = 1;
        mPageCount = 0;

        mKeyBuffer.clear();
        mColumns.clear();
        mKnownColumns.clear();
        mRowBuffer.clear();

        pages.getItems().clear();

        dataTable.getItems().clear();
        dataTable.getColumns().clear();

        recordDetails.setText("");

        //start scan
        mSetInfo = (SetInfo) rootPane.getUserData();
        thisTab = Browser.browserInstance.findTab(mSetInfo.getId().toString());

        TableColumn<RecordRow, Number> column = new TableColumn<RecordRow, Number>("#");
        column.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<RecordRow, Number>, ObservableValue<Number>>() {

            @Override
            public ObservableValue<Number> call(TableColumn.CellDataFeatures<RecordRow, Number> param) {
                RecordRow recordRow = param.getValue();
                if (recordRow != null) {
                    return new SimpleIntegerProperty(recordRow.index);
                }
                return new SimpleIntegerProperty(0);
            }
        });

        dataTable.getColumns().add(column);

        TableColumn<RecordRow, String> StringColumn = new TableColumn<RecordRow, String>("Digest");
        StringColumn.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<RecordRow, String>, ObservableValue<String>>() {

            @Override
            public ObservableValue<String> call(TableColumn.CellDataFeatures<RecordRow, String> param) {
                RecordRow recordRow = param.getValue();
                if (recordRow != null) {
                    return new SimpleStringProperty(Base64.encode(recordRow.getKey().digest));
                }
                return new SimpleStringProperty("");
            }
        });
        dataTable.getColumns().add(StringColumn);

        pages.setCellFactory(new Callback<ListView<Integer>, ListCell<Integer>>() {
            @Override
            public ListCell<Integer> call(ListView<Integer> listView) {
                return new PageRootCell();
            }
        });


        mScanThread = new Thread(App.SCANS, new Runnable() {
            public void run() {
                if (mScanThread != null && rootPane.getScene() != null) {
                    try {
                        mRootDir = new File(System.getProperty("java.io.tmpdir"));
                        mRootDir = new File(mRootDir, "clairvoyance");
                        mRootDir = new File(mRootDir, FileUtil.prettyFileName(mSetInfo.namespace, null, false));
                        mRootDir = new File(mRootDir, FileUtil.prettyFileName(mSetInfo.name, null, false));
                        mRootDir.mkdirs();

                        App.APP_LOGGER.info(mRootDir.getPath());

                        ScanPolicy scanPolicy = new ScanPolicy();
                        scanPolicy.concurrentNodes = true;

                        AerospikeClient client = App.getClient();


                        Statement statement = new Statement();
                        statement.setNamespace(mSetInfo.namespace);
                        statement.setSetName(mSetInfo.name);


                        for (Node node : client.getNodes()) {
                            if (mScanThread != null && rootPane.getScene() != null && !cancelled) {

                                App.APP_LOGGER.info(mSetInfo.name + " start scan on " + node.getHost());
                                RecordSet rs = client.queryNode(null, statement, node);
                                try {
                                    while (!cancelled && mScanThread != null && rootPane.getScene() != null && rs.next()) {
                                        Key key = rs.getKey();
                                        Record record = rs.getRecord();
                                        //System.out.println(key + " " + record);
                                        SetController.this.scanCallback(key, record);
                                    }
                                } finally {
                                    rs.close();
                                    App.APP_LOGGER.info(mSetInfo.name + " scan complete on " + node.getHost());
                                }
                            }
                        }

                        /*
                        for ( Node node : client.getNodes() ) {
                            if ( mScanThread != null && rootPane.getScene() != null && !cancelled ) {
                                App.APP_LOGGER.info(mSetInfo.name + " start scan on " + node.getHost());
                                client.scanNode(null, node, mSetInfo.namespace, mSetInfo.name, SetController.this);
                                App.APP_LOGGER.info(mSetInfo.name + " scan complete on " + node.getHost());
                            }
                        }*/

                    } catch (AerospikeException.ScanTerminated e) {
                        App.APP_LOGGER.info(e.getMessage());
                        return;
                    } catch (Exception e) {
                        App.APP_LOGGER.log(Level.SEVERE, e.getMessage(), e);
                    } finally {
                        App.APP_LOGGER.info(mSetInfo.name + " scans complete");
                    }

                    flushColumns();
                    flush(Thread.currentThread());
                    flushKeys();
                }
            }

        });
        mScanThread.setDaemon(true);

        mCurrentLoader = mScanThread;
        mScanThread.start();
    }

    private void createContextMenu() {
        dataTable.setRowFactory(tableView -> {
            TableRow<RecordRow> tableRow = new TableRow<RecordRow>();

            ContextMenu contextMenuOnExists = new ContextMenu();
            MenuItem menuItemInsertOnExists = new MenuItem("Insert");
            MenuItem menuItemEditOnExists = new MenuItem("Edit");
            MenuItem menuItemDeleteOnExists = new MenuItem("Delete");
            contextMenuOnExists.getItems().addAll(menuItemInsertOnExists, menuItemEditOnExists, menuItemDeleteOnExists);

            menuItemInsertOnExists.setOnAction(event -> {
                if (mSetInfo != null) {
                    Browser.browserInstance.addInsertDocumentTab(mSetInfo.namespace, mSetInfo.name, "");
                } else {
                    Browser.browserInstance.addInsertDocumentTab();
                }
            });
            menuItemEditOnExists.setOnAction(event -> {
                if (mSetInfo != null) {
                    Browser.browserInstance.addInsertDocumentTab(mSetInfo.namespace, mSetInfo.name, (mSetInfo.valueDumped != null ? mSetInfo.valueDumped : ""));
                } else {
                    Browser.browserInstance.addInsertDocumentTab();
                }
            });
            menuItemDeleteOnExists.setOnAction(event -> {
                try {
                    crudService.delete(
                            tableRow.getItem().getKey(),
                            (key, b) -> {
                                App.APP_LOGGER.info(String.format("delete success: %s %s %s", key.namespace, key.setName, key.userKey));
                                this.deleteRecordRowOnSet(key);
                                Alert alert = new Alert(Alert.AlertType.INFORMATION, "Delete success");
                                alert.showAndWait();
                            }, exception -> {
                                App.APP_LOGGER.severe(String.format("delete failed: %s %s %s: " + exception.toString(), tableRow.getItem().getKey().namespace, tableRow.getItem().getKey().setName, tableRow.getItem().getKey().userKey));
                                exception.printStackTrace();
                                Alert alert = new Alert(Alert.AlertType.ERROR, "Delete failed:\n" + exception);
                                alert.showAndWait();
                            }
                    );
                } catch (Exception exception) {
                    App.APP_LOGGER.severe(String.format("delete failed: %s %s %s: " + exception.toString(), tableRow.getItem().getKey().namespace, tableRow.getItem().getKey().setName, tableRow.getItem().getKey().userKey));
                    exception.printStackTrace();
                    Alert alert = new Alert(Alert.AlertType.ERROR, "Delete failed:\n" + exception);
                    alert.showAndWait();
                }
            });

            ContextMenu contextMenuOnNotExists = new ContextMenu();
            MenuItem menuItemInsertOnNotExists = new MenuItem("Insert");
            contextMenuOnNotExists.getItems().addAll(menuItemInsertOnNotExists);

            menuItemInsertOnNotExists.setOnAction(event -> {
                Browser.browserInstance.addInsertDocumentTab();
            });

            tableRow.contextMenuProperty().bind(
                    Bindings.when(tableRow.emptyProperty())
                            .then(contextMenuOnNotExists)
                            .otherwise(contextMenuOnExists)
            );

            return tableRow;
        });
    }

    private void loadPage(final int pageNumber) {
        App.APP_LOGGER.info("Load requested for " + pageNumber);

        //mRowBuffer = new ArrayList<RecordRow>();
        //final List<RecordRow> list = mRowBuffer;
        final File file = new File(mRootDir, pageNumber + ".data");
        if (file.exists()) {
            dataTable.getItems().clear();
            dataTable.getItems().removeAll(dataTable.getItems());

            Thread thread = new Thread(App.LOADS, new Runnable() {
                public void run() {
                    if (Thread.currentThread() == mCurrentLoader && rootPane.getScene() != null) {

                        List<byte[]> keys = new ArrayList<byte[]>();

                        FileInputStream fis = null;
                        DataInputStream dis = null;
                        byte[] digest;
                        try {
                            //change to batch request
                            //AerospikeClient client = App.getClient();

                            fis = new FileInputStream(file);
                            dis = new DataInputStream(fis);

                            int recordIndex = 1 + (pageNumber * mMaxKeyBufferSize);
                            RecordRow recordRow;
                            Key key;
                            Record record = null;
                            //list.clear();
                            mRowBuffer = new ArrayList<RecordRow>();
                            do {
                                digest = new byte[DIGEST_LEN];
                                dis.readFully(digest);
                                keys.add(digest);

                                key = new Key(mSetInfo.namespace, digest, mSetInfo.name, null);
                                //record = client.get(null, key);

                                recordRow = new RecordRow(key, record);
                                recordRow.index = recordIndex++;
                                if (Thread.currentThread() == mCurrentLoader) {
                                    mRowBuffer.add(recordRow);
                                }
    			    			
    			    			/*
    			    			if ( record != null ) {
    			    				mColumns.addAll(record.bins.keySet());
    			    			}*/

                                if (mRowBuffer.size() >= mMaxBufferSize) {
                                    flush(Thread.currentThread());
                                }
                            } while (Thread.currentThread() == mCurrentLoader);

                        } catch (EOFException eof) {
                            App.APP_LOGGER.info("Reached end of " + file.getAbsolutePath());
                        } catch (IOException e) {
                            App.APP_LOGGER.log(Level.SEVERE, e.getMessage(), e);
                        } finally {
                            try {
                                if (dis != null) dis.close();
                            } catch (Exception e) {
                            }
                            try {
                                if (fis != null) fis.close();
                            } catch (Exception e) {
                            }
                        }

                        flush(Thread.currentThread());
                    }
                }

            }, file.getPath());
            thread.setDaemon(true);

            mCurrentLoader = thread;
            thread.start();
        }
    }

    @Override
    public void scanCallback(Key key, Record record) throws AerospikeException {

        if (mScanThread == null || rootPane.getScene() == null || cancelled) {
            throw new AerospikeException.ScanTerminated();
        }

        mColumns.addAll(record.bins.keySet());

        RecordRow recordRow = new RecordRow(key, record);
        recordRow.index = mRecordCount++;

        if (mScanThread == mCurrentLoader && recordRow.index <= mMaxPageZeroSize) {
            mRowBuffer.add(recordRow);
        }

        //System.err.println(mRowBuffer.size() + " " + mMaxBufferSize + " budder " + ( mRowBuffer.size() >= mMaxBufferSize));
        if (mRowBuffer.size() >= mMaxBufferSize) {
            flushColumns();
            flush(mScanThread);
        }

        mKeyBuffer.add(key.digest);
        if (mKeyBuffer.size() >= mMaxKeyBufferSize) {
            flushKeys();
        }
    }

    private void flushKeys() {

        final int pageNumber = mPageCount++;
        final File file = new File(mRootDir, pageNumber + ".data");

        final List<byte[]> keys = mKeyBuffer;
        mKeyBuffer = new ArrayList<byte[]>();

        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(fos);

            for (byte[] digest : keys) {
                if (cancelled) {
                    return;
                }
                dos.write(digest);
            }

            dos.flush();

            Platform.runLater(new Runnable() {
                public void run() {
                    try {
                        pages.getItems().add(pageNumber);
                    } catch (Exception e) {

                    }
                }
            });

        } catch (Exception e) {
            App.APP_LOGGER.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            try {
                if (dos != null) dos.close();
            } catch (Exception e) {

            }

            try {
                if (fos != null) fos.close();
            } catch (Exception e) {

            }

            try {
                FileUtils.forceDeleteOnExit(file);
            } catch (IOException e) {
            }
        }
    }


    private void flushColumns() {

        final Set<String> columns = mColumns;
        mColumns = new HashSet<String>();

        Platform.runLater(new Runnable() {
            public void run() {
                try {
                    TableColumn<RecordRow, String> column;

                    for (String s : columns) {
                        if (!mKnownColumns.contains(s)) {

                            column = new TableColumn<RecordRow, String>(s);
                            column.setMinWidth(50);
                            column.setCellValueFactory(new NoSQLCellFactory(s));

                            mKnownColumns.add(s);
                            dataTable.getColumns().add(column);
                        }
                    }
                } catch (Exception e) {
                    App.APP_LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        });
    }

    private void flush(final Thread thread) {

        final List<RecordRow> list = mRowBuffer;
        mRowBuffer = new ArrayList<RecordRow>(1024);

        Platform.runLater(new Runnable() {
            public void run() {

                try {

                    if (mCurrentLoader == thread) {

                        for (RecordRow rr : list) {
                            if (mCurrentLoader == thread) {
                                dataTable.getItems().add(rr);
                            } else {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    App.APP_LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }

                createContextMenu();
            }
        });
    }

    @FXML
    protected void handleAction(ActionEvent event) {
        App.APP_LOGGER.info(event.toString());
    }

    @FXML
    public void handleBtnRefresh(ActionEvent actionEvent) {
        this.startScan();
    }

    @FXML
    public void handleTxtSearch(KeyEvent keyEvent) {
        if (keyEvent.getCode() == KeyCode.ENTER) {
            if (!chkRegex.isSelected()) {
                for (int i = 0; i < this.dataTable.getItems().size(); ++i) {
                    int index;
                    if (this.dataTable.getSelectionModel().getSelectedIndex() >= 0) {
                        index = (i + this.dataTable.getSelectionModel().getSelectedIndex() + 1) % this.dataTable.getItems().size();
                    } else {
                        index = i;
                    }
                    try {
                        if (
                            mObjectWriter.writeValueAsString(this.dataTable.getItems().get(index)).toLowerCase(Locale.ROOT).contains(txtSearch.getText().toLowerCase(Locale.ROOT))
                        ) {
                            this.dataTable.getSelectionModel().select(index);
                            this.dataTable.refresh();
                            this.dataTable.scrollTo(index);
                            return;
                        }
                    } catch (Exception ignore) {

                    }
                }
            } else {
                Pattern pattern = Pattern.compile(txtSearch.getText());
                for (int i = 0; i < this.dataTable.getItems().size(); ++i) {
                    int index;
                    if (this.dataTable.getSelectionModel().getSelectedIndex() >= 0) {
                        index = (i + this.dataTable.getSelectionModel().getSelectedIndex() + 1) % this.dataTable.getItems().size();
                    } else {
                        index = i;
                    }
                    try {
                        if (
                                pattern.matcher(mObjectWriter.writeValueAsString(this.dataTable.getItems().get(index))).find()
                        ) {
                            this.dataTable.getSelectionModel().select(index);
                            this.dataTable.refresh();
                            this.dataTable.scrollTo(index);
                            return;
                        }
                    } catch (Exception ignore) {

                    }
                }
            }
        }
    }

    @FXML
    public void handleChkRegex(ActionEvent actionEvent) {

    }

    static class PageRootCell extends ListCell<Integer> {
        @Override
        public void updateItem(Integer item, boolean empty) {
            super.updateItem(item, empty);
            if (item == null) {
                this.setText(null);
            } else {
                this.setText(StringUtils.leftPad(Integer.toString(item, Character.MAX_RADIX).toUpperCase(), 4, "0"));
            }
        }
    }

    public void updateRecordRowOnSet(Key key, Map<String, Object> bins) {
        Pair<RecordRow, Integer> recordRowMatches = findRecordRow(key);
        if (recordRowMatches != null) {
            RecordRow newRecordRow = new RecordRow(
                    key,
                    new Record(bins, recordRowMatches.getKey().getRecord().generation + 1, recordRowMatches.getKey().getRecord().expiration)
            ) {{
                index = recordRowMatches.getKey().index;
            }};
            this.dataTable.getItems().set(recordRowMatches.getValue(), newRecordRow);
            // mRowBuffer.set(recordRowMatches.getValue(), newRecordRow);
        } else {
            RecordRow newRecordRow = new RecordRow(
                    key,
                    new Record(bins, 0, 0)
            ) {{
                index = SetController.this.dataTable.getItems().get(SetController.this.dataTable.getItems().size() - 1).index + 1;
            }};
            this.dataTable.getItems().add(newRecordRow);
            // mRowBuffer.add(newRecordRow);
        }
        this.dataTable.refresh();
    }

    public void deleteRecordRowOnSet(Key key) {
        Pair<RecordRow, Integer> recordRowMatches = findRecordRow(key);
        if (recordRowMatches != null) {
            this.dataTable.getItems().remove(recordRowMatches.getKey());
            // mRowBuffer.remove(recordRowMatches.getKey());
            for (int i = recordRowMatches.getValue(); i < this.dataTable.getItems().size(); ++i) {
                --this.dataTable.getItems().get(i).index;
                // --mRowBuffer.get(i).index;
            }
            this.dataTable.refresh();
        }
    }

    public Pair<RecordRow, Integer> findRecordRow(Key key) {
        int index = 0;
        for (RecordRow recordRow : this.dataTable.getItems()) {
            try {
                if (
                        Objects.equals(recordRow.key.namespace, key.namespace) &&
                                Objects.equals(recordRow.key.setName, key.setName) &&
                                Arrays.equals(recordRow.key.digest, key.digest)
                ) {
                    return new Pair(recordRow, index);
                }
            } catch (Exception ignored) {

            }
            index++;
        }
        return null;
    }
}
