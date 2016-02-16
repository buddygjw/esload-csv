package com.teligen.bigdata.esload;

import com.lmax.disruptor.EventFactory;

/**
 * Created by root on 2015/6/24.
 */
public class ZCEvent {
    private String eventdate;
    private String inta;
    private String intb;
    private String shortinta;
    private String shortintb;
    private String chara;
    private String charb;
    private String charc;
    private String booleana;
    private String chard;
    private String chare;
    private String booleanb;
    private String booleanc;
    private String charf;

    public String getEventdate() {
        return eventdate;
    }

    public void setEventdate(String eventdate) {
        this.eventdate = eventdate;
    }

    public String getInta() {
        return inta;
    }

    public void setInta(String inta) {
        this.inta = inta;
    }

    public String getIntb() {
        return intb;
    }

    public void setIntb(String intb) {
        this.intb = intb;
    }

    public String getShortinta() {
        return shortinta;
    }

    public void setShortinta(String shortinta) {
        this.shortinta = shortinta;
    }

    public String getShortintb() {
        return shortintb;
    }

    public void setShortintb(String shortintb) {
        this.shortintb = shortintb;
    }

    public String getChara() {
        return chara;
    }

    public void setChara(String chara) {
        this.chara = chara;
    }

    public String getCharb() {
        return charb;
    }

    public void setCharb(String charb) {
        this.charb = charb;
    }

    public String getCharc() {
        return charc;
    }

    public void setCharc(String charc) {
        this.charc = charc;
    }

    public String getBooleana() {
        return booleana;
    }

    public void setBooleana(String booleana) {
        this.booleana = booleana;
    }

    public String getChard() {
        return chard;
    }

    public void setChard(String chard) {
        this.chard = chard;
    }

    public String getChare() {
        return chare;
    }

    public void setChare(String chare) {
        this.chare = chare;
    }

    public String getBooleanb() {
        return booleanb;
    }

    public void setBooleanb(String booleanb) {
        this.booleanb = booleanb;
    }

    public String getBooleanc() {
        return booleanc;
    }

    public void setBooleanc(String booleanc) {
        this.booleanc = booleanc;
    }

    public String getCharf() {
        return charf;
    }

    public void setCharf(String charf) {
        this.charf = charf;
    }

    public static final EventFactory<ZCEvent> EVENT_FACTORY = new EventFactory<ZCEvent>()
    {
        public ZCEvent newInstance()
        {
            return new ZCEvent();
        }
    };
}
