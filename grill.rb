#
# 
#
# Generated by <a href="http://enunciate.codehaus.org">Enunciate</a>.
#
require 'json'

# adding necessary json serialization methods to standard classes.
class Object
  def to_jaxb_json_hash
    return self
  end
  def self.from_json o
    return o
  end
end

class String
  def self.from_json o
    return o
  end
end

class Boolean
  def self.from_json o
    return o
  end
end

class Numeric
  def self.from_json o
    return o
  end
end

class Time
  #json time is represented as number of milliseconds since epoch
  def to_jaxb_json_hash
    return (to_i * 1000) + (usec / 1000)
  end
  def self.from_json o
    if o.nil?
      return nil
    else
      return Time.at(o / 1000, (o % 1000) * 1000)
    end
  end
end

class Array
  def to_jaxb_json_hash
    a = Array.new
    each { | _item | a.push _item.to_jaxb_json_hash }
    return a
  end
end

class Hash
  def to_jaxb_json_hash
    h = Hash.new
    each { | _key, _value | h[_key.to_jaxb_json_hash] = _value.to_jaxb_json_hash }
    return h
  end
end


module Com

module Inmobi

module Grill

module Api

  # 
  class GrillConf 

    # (no documentation provided)
    attr_accessor :properties

    # the json hash for this GrillConf
    def to_jaxb_json_hash
      _h = {}
      _h['properties'] = properties.to_jaxb_json_hash unless properties.nil?
      return _h
    end

    # the json (string form) for this GrillConf
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this GrillConf with a json hash
    def init_jaxb_json_hash(_o)
      @properties = Hash.from_json(_o['properties']) unless _o['properties'].nil?
    end

    # constructs a GrillConf from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

  # 
  class GrillSessionHandle 

    # (no documentation provided)
    attr_accessor :publicId
    # (no documentation provided)
    attr_accessor :secretId

    # the json hash for this GrillSessionHandle
    def to_jaxb_json_hash
      _h = {}
      _h['publicId'] = publicId.to_jaxb_json_hash unless publicId.nil?
      _h['secretId'] = secretId.to_jaxb_json_hash unless secretId.nil?
      return _h
    end

    # the json (string form) for this GrillSessionHandle
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this GrillSessionHandle with a json hash
    def init_jaxb_json_hash(_o)
      @publicId = String.from_json(_o['publicId']) unless _o['publicId'].nil?
      @secretId = String.from_json(_o['secretId']) unless _o['secretId'].nil?
    end

    # constructs a GrillSessionHandle from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class GrillPreparedQuery 

    # (no documentation provided)
    attr_accessor :prepareHandle
    # (no documentation provided)
    attr_accessor :preparedUser
    # (no documentation provided)
    attr_accessor :conf
    # (no documentation provided)
    attr_accessor :userQuery
    # (no documentation provided)
    attr_accessor :driverQuery
    # (no documentation provided)
    attr_accessor :preparedTime
    # (no documentation provided)
    attr_accessor :selectedDriverClassName

    # the json hash for this GrillPreparedQuery
    def to_jaxb_json_hash
      _h = {}
      _h['prepareHandle'] = prepareHandle.to_jaxb_json_hash unless prepareHandle.nil?
      _h['preparedUser'] = preparedUser.to_jaxb_json_hash unless preparedUser.nil?
      _h['conf'] = conf.to_jaxb_json_hash unless conf.nil?
      _h['userQuery'] = userQuery.to_jaxb_json_hash unless userQuery.nil?
      _h['driverQuery'] = driverQuery.to_jaxb_json_hash unless driverQuery.nil?
      _h['preparedTime'] = preparedTime.to_jaxb_json_hash unless preparedTime.nil?
      _h['selectedDriverClassName'] = selectedDriverClassName.to_jaxb_json_hash unless selectedDriverClassName.nil?
      return _h
    end

    # the json (string form) for this GrillPreparedQuery
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this GrillPreparedQuery with a json hash
    def init_jaxb_json_hash(_o)
      @prepareHandle = Com::Inmobi::Grill::Api::Query::QueryPrepareHandle.from_json(_o['prepareHandle']) unless _o['prepareHandle'].nil?
      @preparedUser = String.from_json(_o['preparedUser']) unless _o['preparedUser'].nil?
      @conf = Com::Inmobi::Grill::Api::GrillConf.from_json(_o['conf']) unless _o['conf'].nil?
      @userQuery = String.from_json(_o['userQuery']) unless _o['userQuery'].nil?
      @driverQuery = String.from_json(_o['driverQuery']) unless _o['driverQuery'].nil?
      @preparedTime = Time.from_json(_o['preparedTime']) unless _o['preparedTime'].nil?
      @selectedDriverClassName = String.from_json(_o['selectedDriverClassName']) unless _o['selectedDriverClassName'].nil?
    end

    # constructs a GrillPreparedQuery from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryResult 


    # the json hash for this QueryResult
    def to_jaxb_json_hash
      _h = {}
      return _h
    end

    # the json (string form) for this QueryResult
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this QueryResult with a json hash
    def init_jaxb_json_hash(_o)
    end

    # constructs a QueryResult from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class GrillQuery 

    # (no documentation provided)
    attr_accessor :driverStartTime
    # (no documentation provided)
    attr_accessor :queryHandle
    # (no documentation provided)
    attr_accessor :driverQuery
    # (no documentation provided)
    attr_accessor :priority
    # (no documentation provided)
    attr_accessor :status
    # (no documentation provided)
    attr_accessor :closedTime
    # (no documentation provided)
    attr_accessor :isPersistent
    # (no documentation provided)
    attr_accessor :queryConf
    # (no documentation provided)
    attr_accessor :submittedUser
    # (no documentation provided)
    attr_accessor :finishTime
    # (no documentation provided)
    attr_accessor :submissionTime
    # (no documentation provided)
    attr_accessor :selectedDriverClassName
    # (no documentation provided)
    attr_accessor :resultSetPath
    # (no documentation provided)
    attr_accessor :launchTime
    # (no documentation provided)
    attr_accessor :driverFinishTime
    # (no documentation provided)
    attr_accessor :userQuery
    # (no documentation provided)
    attr_accessor :driverOpHandle

    # the json hash for this GrillQuery
    def to_jaxb_json_hash
      _h = {}
      _h['driverStartTime'] = driverStartTime.to_jaxb_json_hash unless driverStartTime.nil?
      _h['queryHandle'] = queryHandle.to_jaxb_json_hash unless queryHandle.nil?
      _h['driverQuery'] = driverQuery.to_jaxb_json_hash unless driverQuery.nil?
      _h['priority'] = priority.to_jaxb_json_hash unless priority.nil?
      _h['status'] = status.to_jaxb_json_hash unless status.nil?
      _h['closedTime'] = closedTime.to_jaxb_json_hash unless closedTime.nil?
      _h['isPersistent'] = isPersistent.to_jaxb_json_hash unless isPersistent.nil?
      _h['queryConf'] = queryConf.to_jaxb_json_hash unless queryConf.nil?
      _h['submittedUser'] = submittedUser.to_jaxb_json_hash unless submittedUser.nil?
      _h['finishTime'] = finishTime.to_jaxb_json_hash unless finishTime.nil?
      _h['submissionTime'] = submissionTime.to_jaxb_json_hash unless submissionTime.nil?
      _h['selectedDriverClassName'] = selectedDriverClassName.to_jaxb_json_hash unless selectedDriverClassName.nil?
      _h['resultSetPath'] = resultSetPath.to_jaxb_json_hash unless resultSetPath.nil?
      _h['launchTime'] = launchTime.to_jaxb_json_hash unless launchTime.nil?
      _h['driverFinishTime'] = driverFinishTime.to_jaxb_json_hash unless driverFinishTime.nil?
      _h['userQuery'] = userQuery.to_jaxb_json_hash unless userQuery.nil?
      _h['driverOpHandle'] = driverOpHandle.to_jaxb_json_hash unless driverOpHandle.nil?
      return _h
    end

    # the json (string form) for this GrillQuery
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this GrillQuery with a json hash
    def init_jaxb_json_hash(_o)
      @driverStartTime = Bignum.from_json(_o['driverStartTime']) unless _o['driverStartTime'].nil?
      @queryHandle = Com::Inmobi::Grill::Api::Query::QueryHandle.from_json(_o['queryHandle']) unless _o['queryHandle'].nil?
      @driverQuery = String.from_json(_o['driverQuery']) unless _o['driverQuery'].nil?
      @priority = String.from_json(_o['priority']) unless _o['priority'].nil?
      @status = Com::Inmobi::Grill::Api::Query::QueryStatus.from_json(_o['status']) unless _o['status'].nil?
      @closedTime = Bignum.from_json(_o['closedTime']) unless _o['closedTime'].nil?
      @isPersistent = Boolean.from_json(_o['isPersistent']) unless _o['isPersistent'].nil?
      @queryConf = Com::Inmobi::Grill::Api::GrillConf.from_json(_o['queryConf']) unless _o['queryConf'].nil?
      @submittedUser = String.from_json(_o['submittedUser']) unless _o['submittedUser'].nil?
      @finishTime = Bignum.from_json(_o['finishTime']) unless _o['finishTime'].nil?
      @submissionTime = Bignum.from_json(_o['submissionTime']) unless _o['submissionTime'].nil?
      @selectedDriverClassName = String.from_json(_o['selectedDriverClassName']) unless _o['selectedDriverClassName'].nil?
      @resultSetPath = String.from_json(_o['resultSetPath']) unless _o['resultSetPath'].nil?
      @launchTime = Bignum.from_json(_o['launchTime']) unless _o['launchTime'].nil?
      @driverFinishTime = Bignum.from_json(_o['driverFinishTime']) unless _o['driverFinishTime'].nil?
      @userQuery = String.from_json(_o['userQuery']) unless _o['userQuery'].nil?
      @driverOpHandle = String.from_json(_o['driverOpHandle']) unless _o['driverOpHandle'].nil?
    end

    # constructs a GrillQuery from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryCost 

    # (no documentation provided)
    attr_accessor :estimatedExecTimeMillis
    # (no documentation provided)
    attr_accessor :estimatedResourceUsage

    # the json hash for this QueryCost
    def to_jaxb_json_hash
      _h = {}
      _h['estimatedExecTimeMillis'] = estimatedExecTimeMillis.to_jaxb_json_hash unless estimatedExecTimeMillis.nil?
      _h['estimatedResourceUsage'] = estimatedResourceUsage.to_jaxb_json_hash unless estimatedResourceUsage.nil?
      return _h
    end

    # the json (string form) for this QueryCost
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this QueryCost with a json hash
    def init_jaxb_json_hash(_o)
      @estimatedExecTimeMillis = Bignum.from_json(_o['estimatedExecTimeMillis']) unless _o['estimatedExecTimeMillis'].nil?
      @estimatedResourceUsage = Float.from_json(_o['estimatedResourceUsage']) unless _o['estimatedResourceUsage'].nil?
    end

    # constructs a QueryCost from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

  # 
  class StringList 

    # (no documentation provided)
    attr_accessor :elements

    # the json hash for this StringList
    def to_jaxb_json_hash
      _h = {}
      if !elements.nil?
        _ha = Array.new
        elements.each { | _item | _ha.push _item.to_jaxb_json_hash }
        _h['elements'] = _ha
      end
      return _h
    end

    # the json (string form) for this StringList
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this StringList with a json hash
    def init_jaxb_json_hash(_o)
      if !_o['elements'].nil?
        @elements = Array.new
        _oa = _o['elements']
        _oa.each { | _item | @elements.push String.from_json(_item) }
      end
    end

    # constructs a StringList from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryResultSetMetadata 

    # (no documentation provided)
    attr_accessor :columns

    # the json hash for this QueryResultSetMetadata
    def to_jaxb_json_hash
      _h = {}
      if !columns.nil?
        _ha = Array.new
        columns.each { | _item | _ha.push _item.to_jaxb_json_hash }
        _h['columns'] = _ha
      end
      return _h
    end

    # the json (string form) for this QueryResultSetMetadata
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this QueryResultSetMetadata with a json hash
    def init_jaxb_json_hash(_o)
      if !_o['columns'].nil?
        @columns = Array.new
        _oa = _o['columns']
        _oa.each { | _item | @columns.push Com::Inmobi::Grill::Api::Query::ResultColumn.from_json(_item) }
      end
    end

    # constructs a QueryResultSetMetadata from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryStatus 

    # (no documentation provided)
    attr_accessor :status
    # (no documentation provided)
    attr_accessor :statusMessage
    # (no documentation provided)
    attr_accessor :errorMessage
    # (no documentation provided)
    attr_accessor :isResultSetAvailable
    # (no documentation provided)
    attr_accessor :progress
    # (no documentation provided)
    attr_accessor :progressMessage

    # the json hash for this QueryStatus
    def to_jaxb_json_hash
      _h = {}
      _h['status'] = status.to_jaxb_json_hash unless status.nil?
      _h['statusMessage'] = statusMessage.to_jaxb_json_hash unless statusMessage.nil?
      _h['errorMessage'] = errorMessage.to_jaxb_json_hash unless errorMessage.nil?
      _h['isResultSetAvailable'] = isResultSetAvailable.to_jaxb_json_hash unless isResultSetAvailable.nil?
      _h['progress'] = progress.to_jaxb_json_hash unless progress.nil?
      _h['progressMessage'] = progressMessage.to_jaxb_json_hash unless progressMessage.nil?
      return _h
    end

    # the json (string form) for this QueryStatus
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this QueryStatus with a json hash
    def init_jaxb_json_hash(_o)
      @status = String.from_json(_o['status']) unless _o['status'].nil?
      @statusMessage = String.from_json(_o['statusMessage']) unless _o['statusMessage'].nil?
      @errorMessage = String.from_json(_o['errorMessage']) unless _o['errorMessage'].nil?
      @isResultSetAvailable = Boolean.from_json(_o['isResultSetAvailable']) unless _o['isResultSetAvailable'].nil?
      @progress = Float.from_json(_o['progress']) unless _o['progress'].nil?
      @progressMessage = String.from_json(_o['progressMessage']) unless _o['progressMessage'].nil?
    end

    # constructs a QueryStatus from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class ResultColumn 

    # (no documentation provided)
    attr_accessor :type
    # (no documentation provided)
    attr_accessor :name

    # the json hash for this ResultColumn
    def to_jaxb_json_hash
      _h = {}
      _h['type'] = type.to_jaxb_json_hash unless type.nil?
      _h['name'] = name.to_jaxb_json_hash unless name.nil?
      return _h
    end

    # the json (string form) for this ResultColumn
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this ResultColumn with a json hash
    def init_jaxb_json_hash(_o)
      @type = String.from_json(_o['type']) unless _o['type'].nil?
      @name = String.from_json(_o['name']) unless _o['name'].nil?
    end

    # constructs a ResultColumn from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class ResultRow 

    # (no documentation provided)
    attr_accessor :values

    # the json hash for this ResultRow
    def to_jaxb_json_hash
      _h = {}
      if !values.nil?
        _ha = Array.new
        values.each { | _item | _ha.push _item.to_jaxb_json_hash }
        _h['values'] = _ha
      end
      return _h
    end

    # the json (string form) for this ResultRow
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this ResultRow with a json hash
    def init_jaxb_json_hash(_o)
      if !_o['values'].nil?
        @values = Array.new
        _oa = _o['values']
        _oa.each { | _item | @values.push Object.from_json(_item) }
      end
    end

    # constructs a ResultRow from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QuerySubmitResult 


    # the json hash for this QuerySubmitResult
    def to_jaxb_json_hash
      _h = {}
      return _h
    end

    # the json (string form) for this QuerySubmitResult
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this QuerySubmitResult with a json hash
    def init_jaxb_json_hash(_o)
    end

    # constructs a QuerySubmitResult from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

  # 
  class APIResult 

    # (no documentation provided)
    attr_accessor :message
    # (no documentation provided)
    attr_accessor :status

    # the json hash for this APIResult
    def to_jaxb_json_hash
      _h = {}
      _h['message'] = message.to_jaxb_json_hash unless message.nil?
      _h['status'] = status.to_jaxb_json_hash unless status.nil?
      return _h
    end

    # the json (string form) for this APIResult
    def to_json
      to_jaxb_json_hash.to_json
    end

    #initializes this APIResult with a json hash
    def init_jaxb_json_hash(_o)
      @message = String.from_json(_o['message']) unless _o['message'].nil?
      @status = String.from_json(_o['status']) unless _o['status'].nil?
    end

    # constructs a APIResult from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class PersistentQueryResult < Com::Inmobi::Grill::Api::Query::QueryResult 

    # (no documentation provided)
    attr_accessor :persistedURI
    # (no documentation provided)
    attr_accessor :numRows

    # the json hash for this PersistentQueryResult
    def to_jaxb_json_hash
      _h = super
      _h['persistedURI'] = persistedURI.to_jaxb_json_hash unless persistedURI.nil?
      _h['numRows'] = numRows.to_jaxb_json_hash unless numRows.nil?
      return _h
    end

    #initializes this PersistentQueryResult with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      @persistedURI = String.from_json(_o['persistedURI']) unless _o['persistedURI'].nil?
      @numRows = Fixnum.from_json(_o['numRows']) unless _o['numRows'].nil?
    end

    # constructs a PersistentQueryResult from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryHandleWithResultSet < Com::Inmobi::Grill::Api::Query::QuerySubmitResult 

    # (no documentation provided)
    attr_accessor :queryHandle
    # (no documentation provided)
    attr_accessor :result

    # the json hash for this QueryHandleWithResultSet
    def to_jaxb_json_hash
      _h = super
      _h['queryHandle'] = queryHandle.to_jaxb_json_hash unless queryHandle.nil?
      _h['result'] = result.to_jaxb_json_hash unless result.nil?
      return _h
    end

    #initializes this QueryHandleWithResultSet with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      @queryHandle = Com::Inmobi::Grill::Api::Query::QueryHandle.from_json(_o['queryHandle']) unless _o['queryHandle'].nil?
      @result = Com::Inmobi::Grill::Api::Query::QueryResult.from_json(_o['result']) unless _o['result'].nil?
    end

    # constructs a QueryHandleWithResultSet from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryPlan < Com::Inmobi::Grill::Api::Query::QuerySubmitResult 

    # (no documentation provided)
    attr_accessor :selectWeight
    # (no documentation provided)
    attr_accessor :execMode
    # (no documentation provided)
    attr_accessor :havingWeight
    # (no documentation provided)
    attr_accessor :scanMode
    # (no documentation provided)
    attr_accessor :tablesQueried
    # (no documentation provided)
    attr_accessor :obyWeight
    # (no documentation provided)
    attr_accessor :numSelDi
    # (no documentation provided)
    attr_accessor :numJoins
    # (no documentation provided)
    attr_accessor :planString
    # (no documentation provided)
    attr_accessor :filterWeight
    # (no documentation provided)
    attr_accessor :numObys
    # (no documentation provided)
    attr_accessor :numHaving
    # (no documentation provided)
    attr_accessor :hasError
    # (no documentation provided)
    attr_accessor :hasSubQuery
    # (no documentation provided)
    attr_accessor :numAggrExprs
    # (no documentation provided)
    attr_accessor :numFilters
    # (no documentation provided)
    attr_accessor :errorMsg
    # (no documentation provided)
    attr_accessor :joinWeight
    # (no documentation provided)
    attr_accessor :tableWeights
    # (no documentation provided)
    attr_accessor :queryCost
    # (no documentation provided)
    attr_accessor :gbyWeight
    # (no documentation provided)
    attr_accessor :prepareHandle
    # (no documentation provided)
    attr_accessor :numSels
    # (no documentation provided)
    attr_accessor :numGbys

    # the json hash for this QueryPlan
    def to_jaxb_json_hash
      _h = super
      _h['selectWeight'] = selectWeight.to_jaxb_json_hash unless selectWeight.nil?
      _h['execMode'] = execMode.to_jaxb_json_hash unless execMode.nil?
      _h['havingWeight'] = havingWeight.to_jaxb_json_hash unless havingWeight.nil?
      _h['scanMode'] = scanMode.to_jaxb_json_hash unless scanMode.nil?
      if !tablesQueried.nil?
        _ha = Array.new
        tablesQueried.each { | _item | _ha.push _item.to_jaxb_json_hash }
        _h['tablesQueried'] = _ha
      end
      _h['obyWeight'] = obyWeight.to_jaxb_json_hash unless obyWeight.nil?
      _h['numSelDi'] = numSelDi.to_jaxb_json_hash unless numSelDi.nil?
      _h['numJoins'] = numJoins.to_jaxb_json_hash unless numJoins.nil?
      _h['planString'] = planString.to_jaxb_json_hash unless planString.nil?
      _h['filterWeight'] = filterWeight.to_jaxb_json_hash unless filterWeight.nil?
      _h['numObys'] = numObys.to_jaxb_json_hash unless numObys.nil?
      _h['numHaving'] = numHaving.to_jaxb_json_hash unless numHaving.nil?
      _h['hasError'] = hasError.to_jaxb_json_hash unless hasError.nil?
      _h['hasSubQuery'] = hasSubQuery.to_jaxb_json_hash unless hasSubQuery.nil?
      _h['numAggrExprs'] = numAggrExprs.to_jaxb_json_hash unless numAggrExprs.nil?
      _h['numFilters'] = numFilters.to_jaxb_json_hash unless numFilters.nil?
      _h['errorMsg'] = errorMsg.to_jaxb_json_hash unless errorMsg.nil?
      _h['joinWeight'] = joinWeight.to_jaxb_json_hash unless joinWeight.nil?
      _h['tableWeights'] = tableWeights.to_jaxb_json_hash unless tableWeights.nil?
      _h['queryCost'] = queryCost.to_jaxb_json_hash unless queryCost.nil?
      _h['gbyWeight'] = gbyWeight.to_jaxb_json_hash unless gbyWeight.nil?
      _h['prepareHandle'] = prepareHandle.to_jaxb_json_hash unless prepareHandle.nil?
      _h['numSels'] = numSels.to_jaxb_json_hash unless numSels.nil?
      _h['numGbys'] = numGbys.to_jaxb_json_hash unless numGbys.nil?
      return _h
    end

    #initializes this QueryPlan with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      @selectWeight = Float.from_json(_o['selectWeight']) unless _o['selectWeight'].nil?
      @execMode = String.from_json(_o['execMode']) unless _o['execMode'].nil?
      @havingWeight = Float.from_json(_o['havingWeight']) unless _o['havingWeight'].nil?
      @scanMode = String.from_json(_o['scanMode']) unless _o['scanMode'].nil?
      if !_o['tablesQueried'].nil?
        @tablesQueried = Array.new
        _oa = _o['tablesQueried']
        _oa.each { | _item | @tablesQueried.push String.from_json(_item) }
      end
      @obyWeight = Float.from_json(_o['obyWeight']) unless _o['obyWeight'].nil?
      @numSelDi = Fixnum.from_json(_o['numSelDi']) unless _o['numSelDi'].nil?
      @numJoins = Fixnum.from_json(_o['numJoins']) unless _o['numJoins'].nil?
      @planString = String.from_json(_o['planString']) unless _o['planString'].nil?
      @filterWeight = Float.from_json(_o['filterWeight']) unless _o['filterWeight'].nil?
      @numObys = Fixnum.from_json(_o['numObys']) unless _o['numObys'].nil?
      @numHaving = Fixnum.from_json(_o['numHaving']) unless _o['numHaving'].nil?
      @hasError = Boolean.from_json(_o['hasError']) unless _o['hasError'].nil?
      @hasSubQuery = Boolean.from_json(_o['hasSubQuery']) unless _o['hasSubQuery'].nil?
      @numAggrExprs = Fixnum.from_json(_o['numAggrExprs']) unless _o['numAggrExprs'].nil?
      @numFilters = Fixnum.from_json(_o['numFilters']) unless _o['numFilters'].nil?
      @errorMsg = String.from_json(_o['errorMsg']) unless _o['errorMsg'].nil?
      @joinWeight = Float.from_json(_o['joinWeight']) unless _o['joinWeight'].nil?
      @tableWeights = Hash.from_json(_o['tableWeights']) unless _o['tableWeights'].nil?
      @queryCost = Com::Inmobi::Grill::Api::Query::QueryCost.from_json(_o['queryCost']) unless _o['queryCost'].nil?
      @gbyWeight = Float.from_json(_o['gbyWeight']) unless _o['gbyWeight'].nil?
      @prepareHandle = Com::Inmobi::Grill::Api::Query::QueryPrepareHandle.from_json(_o['prepareHandle']) unless _o['prepareHandle'].nil?
      @numSels = Fixnum.from_json(_o['numSels']) unless _o['numSels'].nil?
      @numGbys = Fixnum.from_json(_o['numGbys']) unless _o['numGbys'].nil?
    end

    # constructs a QueryPlan from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class ResultColumnType

    # (no documentation provided)
    BOOLEAN = "BOOLEAN"

    # (no documentation provided)
    TINYINT = "TINYINT"

    # (no documentation provided)
    SMALLINT = "SMALLINT"

    # (no documentation provided)
    INT = "INT"

    # (no documentation provided)
    BIGINT = "BIGINT"

    # (no documentation provided)
    FLOAT = "FLOAT"

    # (no documentation provided)
    DOUBLE = "DOUBLE"

    # (no documentation provided)
    STRING = "STRING"

    # (no documentation provided)
    TIMESTAMP = "TIMESTAMP"

    # (no documentation provided)
    BINARY = "BINARY"

    # (no documentation provided)
    ARRAY = "ARRAY"

    # (no documentation provided)
    MAP = "MAP"

    # (no documentation provided)
    STRUCT = "STRUCT"

    # (no documentation provided)
    UNIONTYPE = "UNIONTYPE"

    # (no documentation provided)
    USER_DEFINED = "USER_DEFINED"

    # (no documentation provided)
    DECIMAL = "DECIMAL"

    # (no documentation provided)
    NULL = "NULL"

    # (no documentation provided)
    DATE = "DATE"

    # (no documentation provided)
    VARCHAR = "VARCHAR"

    # (no documentation provided)
    CHAR = "CHAR"
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class Status

    # (no documentation provided)
    NEW = "NEW"

    # (no documentation provided)
    QUEUED = "QUEUED"

    # (no documentation provided)
    LAUNCHED = "LAUNCHED"

    # (no documentation provided)
    RUNNING = "RUNNING"

    # (no documentation provided)
    EXECUTED = "EXECUTED"

    # (no documentation provided)
    SUCCESSFUL = "SUCCESSFUL"

    # (no documentation provided)
    FAILED = "FAILED"

    # (no documentation provided)
    CANCELED = "CANCELED"

    # (no documentation provided)
    CLOSED = "CLOSED"
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryPrepareHandle < Com::Inmobi::Grill::Api::Query::QuerySubmitResult 

    # (no documentation provided)
    attr_accessor :prepareHandleId

    # the json hash for this QueryPrepareHandle
    def to_jaxb_json_hash
      _h = super
      _h['prepareHandleId'] = prepareHandleId.to_jaxb_json_hash unless prepareHandleId.nil?
      return _h
    end

    #initializes this QueryPrepareHandle with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      @prepareHandleId = String.from_json(_o['prepareHandleId']) unless _o['prepareHandleId'].nil?
    end

    # constructs a QueryPrepareHandle from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class QueryHandle < Com::Inmobi::Grill::Api::Query::QuerySubmitResult 

    # (no documentation provided)
    attr_accessor :handleId

    # the json hash for this QueryHandle
    def to_jaxb_json_hash
      _h = super
      _h['handleId'] = handleId.to_jaxb_json_hash unless handleId.nil?
      return _h
    end

    #initializes this QueryHandle with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      @handleId = String.from_json(_o['handleId']) unless _o['handleId'].nil?
    end

    # constructs a QueryHandle from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

module Query

  # 
  class InMemoryQueryResult < Com::Inmobi::Grill::Api::Query::QueryResult 

    # (no documentation provided)
    attr_accessor :rows

    # the json hash for this InMemoryQueryResult
    def to_jaxb_json_hash
      _h = super
      if !rows.nil?
        _ha = Array.new
        rows.each { | _item | _ha.push _item.to_jaxb_json_hash }
        _h['rows'] = _ha
      end
      return _h
    end

    #initializes this InMemoryQueryResult with a json hash
    def init_jaxb_json_hash(_o)
      super _o
      if !_o['rows'].nil?
        @rows = Array.new
        _oa = _o['rows']
        _oa.each { | _item | @rows.push Com::Inmobi::Grill::Api::Query::ResultRow.from_json(_item) }
      end
    end

    # constructs a InMemoryQueryResult from a (parsed) JSON hash
    def self.from_json(o)
      if o.nil?
        return nil
      else
        inst = new
        inst.init_jaxb_json_hash o
        return inst
      end
    end
  end

end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

  # 
  class Priority

    # (no documentation provided)
    VERY_HIGH = "VERY_HIGH"

    # (no documentation provided)
    HIGH = "HIGH"

    # (no documentation provided)
    NORMAL = "NORMAL"

    # (no documentation provided)
    LOW = "LOW"

    # (no documentation provided)
    VERY_LOW = "VERY_LOW"
  end

end

end

end

end

module Com

module Inmobi

module Grill

module Api

  # 
  class Status

    # (no documentation provided)
    SUCCEEDED = "SUCCEEDED"

    # (no documentation provided)
    PARTIAL = "PARTIAL"

    # (no documentation provided)
    FAILED = "FAILED"
  end

end

end

end

end
