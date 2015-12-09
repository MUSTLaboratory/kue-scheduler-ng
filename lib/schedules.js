"use strict";

var kue = require('kue');

var Job = kue.Job;
var Queue = kue;

var _ = require('lodash');
var async = require('async');
var datejs = require('date.js');
var uuid = require('node-uuid');
var humanInterval = require('human-interval');
var CronTime = require('cron').CronTime;

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
    let jobScheduleData = job.data._shedule;

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

function _jobEventHandler(event, jobId, result) {
    var self = kue.singleton;

    kue.Job.get(jobId, function (err, job) {
        if (err) return;

        if (event == 'complete' || event == 'failed') {
            if (job.data._shedule && job.data._shedule.type == 'recurrent') {
                let clonedJob = _cloneJob(self, job);
                clonedJob.data._shedule.lastRun = new Date(parseInt(job.started_at));
                //console.log('last run:',clonedJob.data._shedule.lastRun);

                _computeNextRunTime(clonedJob, function (result) {
                    if (result instanceof Error) {
                        self.emit('schedule error', result);
                    } else {
                        clonedJob.delay(result).save(function (err) {
                            if (err) {
                                self.emit('schedule error', err);
                            }
                        });
                    }
                });
            }
        }
    });
}

var createQueue = kue.createQueue;
kue.createQueue = function(options) {
    let queue = createQueue.call(kue, options);

    queue.on('job complete', function( jobId, result ) {
        _jobEventHandler('complete', jobId, result);
    });

    queue.on('job failed', function( jobId, error ) {
        _jobEventHandler('failed', jobId, error);
    });

    return queue;
};

Queue.prototype.every = function (recurrence, job, done) {
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

    job.data._shedule = {
        type: 'recurrent',
        recurrence: recurrence
    };

    _computeNextRunTime(job, function (result) {
        if (result instanceof Error) {
            self.emit('schedule error', result);
            done(result);
        } else {
            job.delay(result).save(function (err) {
                if (err) {
                    self.emit('schedule error', err);
                    done(err);
                } else {
                    done(job.id);
                }
            });
        }
    });
};

Queue.prototype.schedule = function (when, job, done) {
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

    job.data._shedule = {
        type: 'once',
        when: when
    };

    let self = this;

    job.delay(when).save(function (err) {
        if (err) {
            self.emit('schedule error', err);
            done(err);
        } else {
            done(job.id);
        }
    });
};

Queue.prototype.now = function (job, done) {
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