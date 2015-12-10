"use strict";

var kue = require('kue'),
    async = require('async'),
    _ = require('lodash');

var Job = kue.Job,
    Queue = kue;

var noop = function() {};

function _uniqueKey(key) {
    return 'unique:job:' + key;
}

Job.prototype._uniqueKey = function() {
    return _uniqueKey(this.data._unique);
};

Job.prototype.uniqueKey = function() {
    return this.data._unique || null;
};

Job.prototype.unique = function (key) {
    var data = this.data || {};

    data._unique = key;

    this.data = data;

    return this;
};

Job.prototype.isUnique = function () {
    return (this.data._unique) ? true : false;
};

Job.prototype._saveUniqueJobKey = function (done) {
    done = done || noop;

    kue.singleton.client.getset(this._uniqueKey(), this.id, function (err, oldJobId) {
        if (err) return done(err);

        if (oldJobId) {
            Job.get(oldJobId, function (err, job) {
                if (job && (job._state == 'inactive' || job._state == 'delayed')) {
                    job.remove(function (err) {
                        done(err);
                    });
                } else {
                    done();
                }
            });
        } else {
            done();
        }
    });
};

var previousSave = null;

if (previousSave === null) {
    previousSave = Job.prototype.save;
}

Job.prototype.save = function (done) {
    done = done || noop;

    var job = this;

    if (this.isUnique()) {
        return previousSave.call(this, function (err) {
            if (err) return done(err);

            job._saveUniqueJobKey(done);
        });
    } else {
        return previousSave.call(this, done);
    }
};

Job._removeUniqueJobKeyById = function (jobId, done) {
    done = done || noop;

    Job.get(jobId, function (err, job) {
        if (err) return done(err);

        if (job.isUnique()) {
            kue.singleton.client.get(job._uniqueKey(), function (err, jobId) {
                if (err) return next(err);

                if (jobId == job.id) {
                    kue.singleton.client.del(job._uniqueKey(), done);
                } else {
                    done();
                }
            });
        } else {
            done();
        }
    });
};

Job.prototype._removeUniqueKey = function (done) {
    done = done || noop;

    Job._removeUniqueJobKeyById(this.id, done);
};

var previousRemove = Job.prototype.remove;
Job.prototype.remove = function(done) {
    done = done || noop;

    async.waterfall([
        function (next) {
            if (this.id) {
                Job.get(this.id, function (err, job) {
                    if (err) {
                        console.log('error while fetching job:',err);
                    }

                    next(err,job);
                });
            } else {
                next(new Error('job id is not set'));
            }
        }.bind(this),

        function(job, next) {
            if (job.isUnique()) {
                kue.singleton.client.get(job._uniqueKey(), function (err, jobId) {
                    if (err) return next(err);

                    if (jobId == job.id) {
                        kue.singleton.client.del(job._uniqueKey(), next);
                    } else {
                        next(null, null);
                    }
                });
            } else {
                next(null, null);
            }
        }.bind(this),

        function(result, next) {
            previousRemove.call(this, next);
        }.bind(this)

    ], function (error, result) {
        done(error, this);
    }.bind(this));

    return this;
};

Queue.prototype.removeUniqueJob = function (key, done) {
    done = done || noop;

    let uniqueKey = _uniqueKey(key);

    let self = this;

    this.client.get(uniqueKey, function (err, jobId) {
        if (err) return done(err);

        if (jobId) {
            Job.remove(jobId, function(err) {
                if (err) return done(err);

                self.client.del(uniqueKey, done);
            });
        } else {
            done();
        }
    });
};

var subscribed = false;
var createQueue = kue.createQueue;
kue.createQueue = function(options) {
    let queue = createQueue.call(kue, options);

    if (!subscribed) {
        subscribed = true;

        queue.on('job start', function (jobId) {
            Job._removeUniqueJobKeyById(jobId, function (err) {
                if (err) {
                    console.log('failed to remove unique key with job id:', jobId, ', error:', err);
                }
            });
        });
    }

    return queue;
};

module.exports = Queue;
