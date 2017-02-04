package model;

import java.io.Serializable;
import net.sf.json.JSONObject;

public class TotalOrderMulticastMessage implements IMessage, Serializable, Comparable<TotalOrderMulticastMessage> {

    private static final long serialVersionUID = 34278371231L;
    private String content;
    private Integer groupId;
    private Integer source;
    private Integer sequence;
    private Integer messageId;
    private Integer totalOrderSequence;

    public Integer getTotalOrderSequence() {
        return totalOrderSequence;
    }

    public void setTotalOrderSequence(Integer totalOrderSequence) {
        this.totalOrderSequence = totalOrderSequence;
    }

    public Integer getMessageId() {
        return messageId;
    }

    public void setMessageId(Integer messageId) {
        this.messageId = messageId;
    }

    private TotalOrderMessageType messageType;

    public TotalOrderMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(TotalOrderMessageType messageType) {
        this.messageType = messageType;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Integer getGroupId() {
        return groupId;
    }

    public void setGroupId(Integer groupId) {
        this.groupId = groupId;
    }

    public Integer getSource() {
        return source;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public Integer getSequence() {
        return sequence;
    }

    public void setSequence(Integer sequence) {
        this.sequence = sequence;
    }

    public boolean isDeliverable() {
        return this.messageType == TotalOrderMessageType.FINAL;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String toString() {
      /*  return String.format('{'+'"mid"'+ ":%d," + '"tos"' + ":%d," +  '"message type"' + ":%s," + '"group"' + ":%d," + '"source"' + 
        ":%d," + '"sequence"' + ":%d," + '"content"' + ":%s"+'}',
                messageId,
                totalOrderSequence,
                messageType,
                groupId,
                source,
                sequence,
                content);
        */        
                
                
                
        JSONObject stringa = new JSONObject();
        stringa.put("mid", messageId) ;
        stringa.put("tos", totalOrderSequence) ;
        stringa.put("message type", messageType) ;  
        stringa.put("group", groupId) ;
        stringa.put("source", source) ;
        stringa.put("sequence", sequence) ;  
        stringa.put("content", content) ; 
        return stringa.toString();
/*
        JsonObjectBuilder job = (JsonObjectBuilder) Json.createObjectBuilder();
        JsonObject jo;
        job2.addNull("id");
        job2.add("exist", false);
        jo2 = job2.build();
*/

    }

    @Override
    public int compareTo(TotalOrderMulticastMessage o) {
        //System.out.println(this.sequence +" v.s "+o.sequence +" ? == "+o.sequence.equals(this.sequence));
        //return this.sequence - o.sequence;
        if (o.sequence.equals(this.sequence) == false) {
            return this.sequence - o.sequence;
        } else {
            if (o.isDeliverable() == false && this.isDeliverable() == true) {
                return 1;
            } else if (o.isDeliverable() == true && this.isDeliverable() == false) {
                return -1;
            } else {
                if (this.source == o.source) {
                    return this.messageId - o.messageId;
                } else {
                    return this.source - o.source;
                }
            }
        }
    }

}
