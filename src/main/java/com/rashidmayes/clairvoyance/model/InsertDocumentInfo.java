package com.rashidmayes.clairvoyance.model;

import com.rashidmayes.clairvoyance.Identifiable;

import java.util.concurrent.atomic.AtomicInteger;

public class InsertDocumentInfo implements Identifiable {
    public static AtomicInteger maxCount = new AtomicInteger(1);
    public int index;

    public String namespace = "";
    public String setName = "";
    public String value = "{\n    \"id\":\"\"\n}";

    public InsertDocumentInfo() {
        this.index = maxCount.getAndIncrement();
    }

    @Override
    public Object getId() { return makeId(index); }
    public Object getSetId() { return "$set." + namespace + "."+ setName; }

    private String makeId(int newIndex) { return "$insert.document." + newIndex;}
}
