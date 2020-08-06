/*
 * Copyright (c) 2009, NSF Cloud and Autonomic Computing Center, Rutgers University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Cloud and Autonomic Computing Center, Rutgers University, nor the names of its
 * contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
*  Ciprian Docan (2009)  TASSL Rutgers University
*  docan@cac.rutgers.edu
*/

#include "timer.h"

#ifdef XT3
static double tv_diff( double start, double end )
{
        return end - start;
}
#else
static double tv_diff( struct timeval* tv_start, struct timeval* tv_end )
{
        // returns the time difference in seconds 
        long sec, usec;

        sec = tv_end->tv_sec - tv_start->tv_sec;
        usec = tv_end->tv_usec - tv_start->tv_usec;
        if( usec < 0 ) {
                sec = sec - 1;
                usec += 1000000;
        }

        return (double) (sec + (double) usec / 1.e6);
}
#endif  // XT3

static void __timer_update( mtimer_t* timer )
{
#ifdef XT3
        double now;

        now = dclock();
        timer->elapsed_time += tv_diff( timer->stoptime, now );
        timer->stoptime = now;
#else
        struct timeval now;

        gettimeofday( &now, 0 );
        timer->elapsed_time += tv_diff( &timer->stop_time, &now );
        timer->stop_time = now;
        // gettimeofday( &timer->stop_time, 0 );
#endif
}

static void __timer_reset(mtimer_t* timer)
{
#ifdef XT3
        timer->starttime = timer->stoptime = dclock();
#else
        gettimeofday(&timer->start_time, 0);
        timer->stop_time = timer->start_time;
#endif
}

void timer_init(mtimer_t* timer, unsigned int alarm_time)
{
        // input parameter "alarm_time" is expressed in mili-seconds
        timer->elapsed_time = 0.0;

        timer->alarm_time = (double) alarm_time / 1.e3;
        timer->stopped = 0;
        timer->started = 0;

        __timer_reset( timer );
}

void timer_start(mtimer_t* timer)
{
        if (!timer->started) {
                timer->started = 1;
                if (!timer->stopped)
                        __timer_reset(timer);
                else
#ifdef XT3
                        timer->stoptime = dclock();
#else
                        gettimeofday(&timer->stop_time, 0);
#endif
        }
        timer->stopped = 0;
}

void timer_stop(mtimer_t *timer)
{
        if (!timer->stopped) {
                __timer_update(timer);
                timer->stopped = 1;
        }
        timer->started = 0;
}

void timer_reset(mtimer_t *timer)
{
        timer->elapsed_time = 0.0;

        __timer_reset(timer);
}

/* Function to read the elapsed time value. It does not stop the
   internal timer. */
double timer_read(mtimer_t *timer)
{
        if (timer->started && !timer->stopped)
                __timer_update(timer);

        return timer->elapsed_time;
}

int timer_expired(mtimer_t* timer)
{
        if( !timer->stopped )
                __timer_update( timer );        

        return timer->alarm_time <= timer->elapsed_time;
}

/* Function to get and return the current (wall clock) time. */
double timer_timestamp(void)
{
        double ret;

#ifdef XT3
        ret = dclock();
#else
        struct timeval tv;

        gettimeofday( &tv, 0 );
        ret = (double) tv.tv_usec + tv.tv_sec * 1.e6;
#endif
        return ret;
}
