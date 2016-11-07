/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import classnames from 'classnames';
import {MyAppApi} from 'api/app';
import {objectQuery} from 'services/helpers';
require('./AppOverview.less');
import OverviewTabConfig from './OverviewTabConfig';
import ConfigurableTab from 'components/ConfigurableTab';
import ApplicationMetrics from 'components/EntityCard/ApplicationMetrics';
import shortid from 'shortid';

export default class AppOverview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      entityDetail: {
        name: '',
        artifact: {
          version: '--',
          name: '--'
        },
        programs: [],
        datasets: [],
        streams: []
      },
      dimension: {}
    };
  }
  getChildContext() {
    return {
      entity: this.state.entityDetail
    };
  }
  componentDidMount() {
    let {right, left, width} = this.overviewRef.parentElement.getBoundingClientRect();
    if (right && left) {
      this.setState({
        dimension: {
          right,
          left: ( left < width ? width : left)
        }
      });
    }
    let namespace = objectQuery(this.props.entity, 'metadata', 'entityId', 'id', 'namespace', 'id');
    let appId = this.props.entity.id;
    MyAppApi
      .get({
        namespace,
        appId
      })
      .map(entityDetail => {
        let programs = entityDetail.programs.map(prog => {
          prog.uniqueId = shortid.generate();
          return prog;
        });
        let datasets = entityDetail.datasets.map(dataset => {
          dataset.entityId = {
            id: {
              instanceId: dataset.name
            },
            type: 'datasetinstance'
          };
          dataset.uniqueId = shortid.generate();
          return dataset;
        });

        let streams = entityDetail.streams.map(stream => {
          stream.entityId = {
            id: {
              streamName: stream.name
            },
            type: 'stream'
          };
          stream.uniqueId = shortid.generate();
          return stream;
        });
        entityDetail.streams = streams;
        entityDetail.datasets = datasets;
        entityDetail.programs = programs;
        return entityDetail;
      })
      .subscribe(entityDetail => {
        this.setState({ entityDetail });
      });
  }
  render() {
    let style={};
    let {left, right} = this.state.dimension;
    let {right: parentRight} = this.props.parentdimension;
    if (this.overviewRef) {
      // FIXME: This is a hack. We need to make this as an elegant component that will intelligently
      // figure out the position based on the intent and containing component.
      if (this.props.position === 'right') {
        style.left = 310;
        style.width = `calc(100vw - ${right + 30}px)`; // factoring the container-fluid on body
      }
      if (this.props.position === 'left') {
        style.right = 310;
        style.width = `calc(100vw - ${(parentRight - left) + 30 + 20}px)`; // factoring the container-fluid on body
      }
      if (this.props.position === 'bottom') {
        style.right = 0;
        style.width = left + 300 - 30;
      }
    }
    return (
      <div
        className={classnames("entity-overview", this.props.position)}
        ref={ref=> this.overviewRef = ref}
        style={style}
        onClick={(e) => e.stopPropagation()}
      >
        <div>
          <div className="overview-header clearfix">
            <span>
              {this.state.entityDetail.name}
              <small>
                {this.state.entityDetail.description}
              </small>
            </span>
            <span>
              {this.state.entityDetail.artifact.name}
              <small>
                Version: {this.state.entityDetail.artifact.version}
              </small>
            </span>
            <span className="text-right">
              <i className="fa fa-info fa-lg"></i>
              <i className="fa fa-arrows-alt"></i>
              <i
                className="fa fa-times fa-lg"
                onClick={this.props.onClose}
              ></i>
            </span>
          </div>
          <div className="overview-content">
            <ApplicationMetrics entity={this.props.entity}/>
            <ConfigurableTab
              tabConfig={OverviewTabConfig}
            />
          </div>
        </div>
      </div>
    );
  }
}
AppOverview.defaultPropTypes = {
  position: 'left'
};

AppOverview.childContextTypes = {
  entity: PropTypes.object
};

AppOverview.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string,
    type: PropTypes.string,
    version: PropTypes.string,
    metadata: PropTypes.object, // FIXME: Shouldn't be an object
    icon: PropTypes.string,
    isHydrator: PropTypes.bool
  }),
  parentdimension: PropTypes.shape({
    left: PropTypes.number,
    right: PropTypes.number,
    bottom: PropTypes.number,
    top: PropTypes.number
  }),
  position: PropTypes.oneOf([
    'left',
    'right',
    'top',
    'bottom'
  ]),
  onClose: PropTypes.func
};
