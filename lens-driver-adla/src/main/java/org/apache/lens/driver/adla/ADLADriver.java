
package org.apache.lens.driver.adla;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;

import org.apache.lens.server.api.driver.AbstractLensDriver;
import org.apache.lens.server.api.driver.DriverEvent;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensResultSet;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.StaticQueryCost;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ADLADriver extends AbstractLensDriver {


    @Override
    public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
        return new StaticQueryCost(1.0d);
    }

    @Override
    public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
        throw new LensException("UnSupported operation estimate");
    }

    @Override
    public void prepare(PreparedQueryContext pContext) throws LensException {
        throw new LensException("UnSupported operation prepare");
    }

    @Override
    public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
        throw new LensException("UnSupported operation explainAndPrepare");
    }

    @Override
    public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
        throw new LensException("UnSupported operation closePreparedQuery");
    }

    @Override
    public LensResultSet execute(QueryContext context) throws LensException {
        throw new LensException("UnSupported operation execute");
    }

    @Override
    public void executeAsync(QueryContext context) throws LensException {
        //Submit ADLA JOB
    }

    @Override
    public void updateStatus(QueryContext context) throws LensException {
       //Update status of ADLA JOB
    }

    @Override
    public void closeResultSet(QueryHandle handle) throws LensException {
        //NO OP
    }

    @Override
    public boolean cancelQuery(QueryHandle handle) throws LensException {
        return false;
    }

    @Override
    public void closeQuery(QueryHandle handle) throws LensException {
        //NO OP
    }

    @Override
    public void close() throws LensException {
        //NO OP
    }

    @Override
    public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {
        // NO OP
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // NO OP
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // NO OP
    }

    @Override
    protected LensResultSet createResultSet(QueryContext ctx) throws LensException {
        //Get JOB result
        return null;
    }

}