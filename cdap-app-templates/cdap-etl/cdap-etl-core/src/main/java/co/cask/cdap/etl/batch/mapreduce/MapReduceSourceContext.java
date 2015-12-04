/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSourceContext;

import java.util.List;
import java.util.Map;

/**
 * MapReduce Source Context.
 */
public class MapReduceSourceContext extends MapReduceBatchContext implements BatchSourceContext {

  public MapReduceSourceContext(MapReduceContext context, Metrics metrics, LookupProvider lookup, String stageName,
                                Map<String, String> runtimeArgs) {
    super(context, metrics, lookup, stageName, runtimeArgs);
  }

  @Override
  public void setInput(StreamBatchReadable stream) {
    mrContext.setInput(stream);
  }

  @Override
  public void setInput(String datasetName) {
    mrContext.setInput(datasetName);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments) {
    mrContext.setInput(datasetName, arguments);
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    mrContext.setInput(datasetName, splits);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments, List<Split> splits) {
    mrContext.setInput(datasetName, arguments, splits);
  }

  @Override
  public void setInput(InputFormatProvider inputFormatProvider) {
    mrContext.setInput(inputFormatProvider);
  }

  @Override
  public void setInput(String datasetName, Dataset dataset) {
    mrContext.setInput(datasetName, dataset);
  }
}
