package org.wso2.siddhi.extension.CustomWin2;

import java.util.ArrayList;
import java.util.List;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

/*
 * This function can be use to remove the duplicate of some attributes by assign 
 * the priority to given attribute value
 * clearDuplicate(WindowSized,PasstoOut,attribute1,attribute2)
 * remove the duplicates of attribute 1 by considering attribute 2 values
 * return the no of events equal to passtoOut count
 */

public class clearDuplicate extends StreamProcessor {
    private int Lengthtokeep;
    private int PassToOut;
    private ArrayList<StreamEvent> ClearedWindow = new ArrayList<StreamEvent>();
    VariableExpressionExecutor variableExpressionExecutor;
    VariableExpressionExecutor variableExpressionComparator;

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

    @Override
    public Object[] currentState() {
        // TODO Auto-generated method stub
        return new Object[] { ClearedWindow };
    }

    @Override
    public void restoreState(Object[] state) {
        // TODO Auto-generated method stub
        ClearedWindow = (ArrayList<StreamEvent>) state[0];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        // TODO Auto-generated method stub
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        boolean isDuplicate;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            isDuplicate = RemoveDuplicate(streamEvent);
            if (!isDuplicate) {
                if (ClearedWindow.size() < Lengthtokeep) {
                    ClearedWindow.add(streamEvent);
                } else {
                    ClearedWindow.add(streamEvent);
                    for (int j = 0; j < PassToOut; j++) {
                        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(ClearedWindow.get(j));
                        complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { j + 1 });
                        returnEventChunk.add(clonedEvent);
                    }
                    nextProcessor.process(returnEventChunk);
                }
            }

        }

    }

    private boolean RemoveDuplicate(StreamEvent event) {
        boolean FoundDuplicate = false;
        for (int i = ClearedWindow.size() - 1; i >= 0; i--) {
            if (variableExpressionExecutor.execute(ClearedWindow.get(i)).equals(
                    variableExpressionExecutor.execute(event))) {
                if ((Double) variableExpressionComparator.execute(ClearedWindow.get(i)) < (Double) variableExpressionComparator
                        .execute(event)) {
                    ClearedWindow.remove(i);
                    ClearedWindow.add(i, event);
                }
                FoundDuplicate = true;
                break;
            }
        }
        return FoundDuplicate;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Lengthtokeep = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            PassToOut = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (!(attributeExpressionExecutors[2] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a other parameter");
        } else {
            variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[2];
        }
        if (!(attributeExpressionExecutors[attributeExpressionExecutors.length - 1] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a other parameter");
        } else {
            variableExpressionComparator = (VariableExpressionExecutor) attributeExpressionExecutors[attributeExpressionExecutors.length - 1];
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        return attributeList;
    }

}
