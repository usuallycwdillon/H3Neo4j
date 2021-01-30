package edu.gmu.cds.cdillon2.results;

import org.neo4j.graphdb.Node;
import java.util.Collections;
import java.util.Map;

/**
 * @author neo4j
 * @since 01.06.2020
 * https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/4.1/core/src/main/java/apoc/result/NodeWithMapResult.java
 * Yet another result object lifted directly from the apoc library
 */

public class NodeWithMapResult {
    public final Node node;
    public final Map<String, Object> value;
    public final Map<String, Object> error;

    public NodeWithMapResult(Node node, Map<String, Object> value, Map<String, Object>  error) {
        this.node = node;
        this.value = value;
        this.error = error;
    }

    public static NodeWithMapResult withError(Node node, Map<String, Object> error) {
        return new NodeWithMapResult(node, Collections.emptyMap(), error);
    }

    public static NodeWithMapResult withResult(Node node, Map<String, Object> value) {
        return new NodeWithMapResult(node, value, Collections.emptyMap());
    }

}
