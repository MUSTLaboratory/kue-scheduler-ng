"use strict";

var kue = require('kue'),
    Warlock = require('node-redis-warlock');

var Job = kue.Job;
var Queue = kue;

var _ = require('lodash');
var async = require('async');
var datejs = require('date.js');
var uuid = require('node-uuid');
var humanInterval = require('human-interval');
var CronTime = require('cron').CronTime;

var noop = function() {};

function _cloneJob(queue, jobDefinition) {
    var jobDefaults = {

    };

    jobDefinition = _.merge(jobDefaults, jobDefinition);

    var job = queue.createJob(jobDefinition.type, jobDefinition.data);

    _.without(_.keys(jobDefinition), 'progress').forEach(function(attribute) {
        var fn = job[attribute];
        var isFunction = !_.isUndefined(fn) && _.isFunction(fn);

        if (isFunction) {
            fn.call(job, jobDefinition[attribute]);
        }
    });

    if (jobDefinition.attempts) {
        job.attempts(jobDefinition.attempts.max);
    }

    if (jobDefinition._max_attempts) {
        job.attempts(jobDefinition._max_attempts);
    }

    return job;
}

function _computeNextRunTime(job, done) {
    let jobScheduleData = job.data._schedule;

    if (!jobScheduleData) {
        return done(new Error('invalid job data'));
    }

    //grab job reccur interval
    var recurrence = jobScheduleData.recurrence;

    try {
        //last run of the job is now if not exist
        var lastRun =
            jobScheduleData.lastRun ? new Date(jobScheduleData.lastRun) : new Date();

        //compute next date from the cron interval
        var cronTime = new CronTime(recurrence);
        var nextRun = cronTime._getNextDateFrom(lastRun);

        // Handle cronTime giving back the same date
        // for the next run time
        if (nextRun.valueOf() === lastRun.valueOf()) {
            nextRun =
                cronTime._getNextDateFrom(
                    new Date(lastRun.valueOf() + 1000)
                );
        }

        return done(nextRun.toDate());
    } catch (ex) {
        //not in cron format, let's try to parse it as a human interval
    }

    try {
        //last run of the job is now if not exist
        var lastRun =
            jobScheduleData.lastRun ? new Date(jobScheduleData.lastRun) : new Date();

        var nextRun =
            new Date(lastRun.valueOf() + humanInterval(recurrence));

        return done(nextRun);
    } catch (ex) {
        //not in human interval format, fire an error
    }

    return done(new Error('invalid recurrence'));
}

Queue.prototype._rescheduleJob = function (job, done) {
    done = done || noop;

    var lockTtl = 2000;
    let jobId = job.id;
    let self = this;

    if (job.data._schedule && job.data._schedule.type == 'recurrent') {
        self.warlock.lock(self.client.getKey('lock:job:reschedule:'+jobId), lockTtl, function( err, unlock ) {
            if (err) {
                self.emit('error', err);
                return done(err);
            }

            if (typeof unlock === 'function') {
                self.client.sismember(self.client.getKey('rescheduled:jobs'), jobId, function (err, result) {
                    if (err) {
                        unlock();
                        done(err);
                        return self.emit('schedule error', err);
                    }

                    if (result != 0) {
                        unlock();
                        done(err);
                        return self.emit('schedule error', new Error('already rescheduled'));
                    }

                    let clonedJob = _cloneJob(self, job);
                    clonedJob.data._schedule.lastRun = new Date(parseInt(job.promote_at));

                    _computeNextRunTime(clonedJob, function (result) {
                        if (result instanceof Error) {
                            unlock();
                            self.emit('schedule error', result);
                            done(err);
                        } else {
                            clonedJob.delay(result).save(function (err) {
                                if (err) {
                                    unlock();
                                    done(err);
                                    return self.emit('schedule error', err);
                                }

                                self.client.sadd(self.client.getKey('rescheduled:jobs'), jobId, function (err) {
                                    unlock();

                                    if (err) {
                                        self.emit('schedule error', err);
                                    }

                                    done(err);
                                });
                            });
                        }
                    });
                });
            }
        });
    }
};

Queue.prototype._jobEventHandler = function (event, jobId, result) {
    var self = this;

    Job.get(jobId, function (err, job) {
        if (err) {
            self.emit('error','error fetching job in jobEventHandler: '+err);
            return;
        }

        if (event == 'start') {
            self._rescheduleJob(job);
        } else if (event == 'complete' || event == 'failed') {
            self.client.srem(self.client.getKey('rescheduled:jobs'), jobId);
        }
    });
};

var subscribed = false;
var createQueue = kue.createQueue;
kue.createQueue = function(options) {
    let queue = createQueue.call(kue, options);

    if (!subscribed) {
        subscribed = true;

        queue.on('job start', function (jobId, result) {
            queue._jobEventHandler('start', jobId, result);
        });

        queue.on('job complete', function (jobId, result) {
            queue._jobEventHandler('complete', jobId, result);
        });

        queue.on('job failed', function (jobId, result) {
            queue._jobEventHandler('failed', jobId, result);
        });

    }

    return queue;
};

Queue.prototype.every = function (recurrence, job, done) {
    done = done || noop;

    let self = this;

    if (!recurrence || !job) {
        let err = new Error('Invalid number of parameters');
        this.emit('schedule error', err);
        done(err);
        return;
    } else if (!(job instanceof Job)) {
        let err = new Error('Invalid job type');
        this.emit('schedule error', err);
        done(err);
        return;
    }

    job.data._schedule = {
        type: 'recurrent',
        recurrence: recurrence
    };

    _computeNextRunTime(job, function (result) {
        if (result instanceof Error) {
            self.emit('schedule error', result);
            return done(result);
        }

        job.delay(result);

        job.save(function (err) {
            if (err) {
                self.emit('schedule error', err);
            }

            done(err);
        });
    });
};

Queue.prototype.schedule = function (when, job, done) {
    done = done || noop;

    let self = this;

    if (!when || !job) {
        let err = new Error('Invalid number of parameters');
        this.emit('schedule error', err);
        done(err);
        return;
    } else if (!(job instanceof Job)) {
        let err = new Error('Invalid job type');
        this.emit('schedule error', err);
        done(err);
        return;
    }

    if (!(when instanceof Date)) {
        try {
            when = datejs(when);
        } catch (error) {
            let err = new Error(error);
            this.emit('schedule error', err);
            done(err);
            return;
        }
    }

    job.data._schedule = {
        type: 'once',
        when: when
    };

    job.delay(when);

    job.save(function (err) {
        if (err) {
            self.emit('schedule error', err);
        }

        done(err);
    });
};

Queue.prototype.now = function (job, done) {
    done = done || noop;

    if (!job || !(job instanceof Job)) {
        let err = new Error('Invalid job type');
        this.emit('schedule error', err);
        done(err);
        return;
    }

    let self = this;

    job.save(function(err){
        if (err) {
            self.emit('schedule error', err);
            done(err);
        } else {
            done(job.id);
        }
    });
};

module.exports = Queue;
