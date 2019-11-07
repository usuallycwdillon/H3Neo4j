package edu.gmu.cds.cdillon2.results;

import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * @author mh
 * @since 26.02.16
 */
public class NodeListResult {
    public final List<Node> nodes;

    public void addNode(Node n) {
        nodes.add(n);
    }

    public NodeListResult(List<Node> value) {
        this.nodes = value;
    }
}