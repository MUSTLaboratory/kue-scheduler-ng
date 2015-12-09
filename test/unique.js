"use strict";

var expect = require('chai').expect;
var moment = require('moment');
var KueScheduler = require('../');

describe("unique", function() {
    this.timeout(60000);

    var queue = KueScheduler.createQueue();

    it('unique double schedule', function (done) {
        let unique = ''+Date.now();

        let job_name = 'test_unique_double_' + unique;

        var job = queue.create(job_name, { some_field: 'with value 1' }).unique(unique);

        queue.schedule('3 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }

            var job2 = queue.create(job_name, { some_field: 'with value 2' }).unique(unique);

            queue.schedule('5 second',job2, function (result) {
                if (result instanceof Error) {
                    done(result);
                }
            });
        });

        queue.process(job_name, function (job, ctx, done_job) {
            console.log("running:",job_name,'unique:',job.uniqueKey());
            done_job();

            expect(job.data.some_field).to.equal('with value 2');

            done();
        });
    });

    it('unique remove', function (done) {
        let unique = ''+Date.now();

        let job_name = 'test_unique_remove_' + unique;

        var job = queue.create(job_name, { some_field: 'with value 1' }).unique(unique);

        queue.schedule('2 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }

            queue.removeUniqueJob(unique, function (err) {
                if (err) return done(err);

                setTimeout(done,3000);
            });
        });

        queue.process(job_name, function (job, ctx, done_job) {
            console.log("running:",job_name,'unique:',job.uniqueKey());
            done_job();
            done(new Error('job shouldn\'t have been started as it is removed'));
        });
    });
});