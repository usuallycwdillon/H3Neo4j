package edu.gmu.cds.cdillon2.schema;


import org.neo4j.graphdb.Node;


public class Computation {
    // (c:Computation{description:'Values imputed in R using package imputeTS', filename:'dataImputations.r'})
    private static final String FILENAME = "filename";
    private final Node underlyingNode;
    private Long timedatestamp;
    private String description;
    private String methodName;


    public Computation(Node node) {
        this.underlyingNode = node;
    }

    public String getFilename() {
        return (String) underlyingNode.getProperty("filename");
    }

    public Node getUnderlyingNode() {
        return this.underlyingNode;
    }

    public Long getTimedatestamp() {
        return (Long) underlyingNode.getProperty("timedatestamp");
    }

    public void setTimedatestamp(Long timedatestamp) {
        this.timedatestamp = timedatestamp;
    }

    public String getDescription() {
        return (String) underlyingNode.getProperty("description");
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMethodName() {
        return (String) underlyingNode.getProperty("methodname");
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Computation && underlyingNode.equals( ((Computation) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Computation[" + getFilename() + "." + getMethodName() + ", " + getTimedatestamp() + "]";
    }
}
