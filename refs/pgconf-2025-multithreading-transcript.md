0:10
Hi everyone, thanks for coming to my talk. ... This is a
0:17
developer conference, so it's about things that don't work yet, and I just want to make that clear. There's
0:24
some ideas here, and I'm also going to be talking about work done by a lot of different people,
0:30
a little bit by me but some a lot by other people, just to make that clear as
0:35
well. Okay so I'll start with some basic concepts and backgrounds. So
0:42
Postgres is really old, and it began in 1985 and there's a
0:51
paper called the implementation of POSTGRES by Stonebraker and crew
0:56
that was written in 1990 so that, you know, the project was only five years old
1:01
but it was still five years before POSIX standardized threads APIs and Windows
1:09
didn't exist uh Windows with threads didn't exist yet, that was 1993. At that time in the late 80s and
1:17
early 90s there were, I guess I should say late 80s, some of the high-end Unix
1:23
systems did have threads but they were all incompatible and so in that paper they just, you know, they mentioned that
1:30
they planned to essentially rebuild POSTGRES using threads at some point when they could and they started with a
1:37
simple process model which we still have today, so we essentially haven't done what the project plans say we should do.
1:44
One of the interesting systems they had was the Sequent Symmetry which is mentioned there, I think they did a lot
1:50
of their work on Sun 4s, but they also had the Sequent Symmetry system which had a huge number of CPUs in a big
1:56
box Finally in 2011 the C standard
2:04
introduced a standard way to do threads, and I'll look at some of these things a little bit later. So that's kind
2:11
of the beginning that I want to start with, and with a bit of background on
2:18
what a multi-process design looks like. So there's essentially three
2:24
different things you could -- three different reasons you might want to start a new thread of execution, and Unix
2:30
traditionally used fork for all, it was the only way to create a
2:36
separate thread of execution, so the first category I'm talking about here is
2:42
if you want to create a multi-process system like Postgres, you use the fork operation -- Windows doesn't have that
2:50
and we'll look in a moment about what consequence that has for us. The second thing you might want to do is run a
2:55
separate a different program, start up a different executable as a subprocess and
3:00
on Unix systems you do that with fork() or vfork() -- I'll talk about that in a moment -- or posix_spawn() which almost no one
3:06
uses. Windows has a CreateProcess() function, it can only create a new
3:13
process running a different executable -- or it could be the same executable but it won't it share anything with
3:18
the parent. And the third thing you might want to do is start a thread in the current process
3:24
so that's, it's a new thread of execution but there's no new process, it's running
3:30
inside the same process and I'll look at what that actually means in a moment. And Windows has that third option, so Windows has 2 and 3 but it doesn't
3:37
have 1, and yet Postgres is built essentially on 1, so one question is
3:43
how on earth does that work? While looking into the history of this stuff I found this paper really
3:49
interesting. It's not that old, it's 2019, it describes how fork() was you
3:58
know, in the beginning of Unix, it wasn't actually original, other operating systems at the time had
4:04
a fork() but they had much more sophisticated fork()s that could control many more things about the sub-, about the
4:09
newly created thread of execution including the memory map and other things like that. It's a very simple
4:15
looking interface but according to these operating system researchers it's essentially transferred an enormous
4:22
amount of complexity and problems to other parts of, or essentially, to the application developer, and you can see
4:28
that all kinds of systems invented variants like vfork(), rfork() and clone() from Linux and rfork()'s from Plan 9 and vfork()'s
4:37
from BSD and became part of POSIX. I'll talk about what those things do in a moment. These researchers say
4:43
that essentially all new systems should have only 2 and 3, and 1 was really like an attractive and simple
4:51
idea that turns out to be a little bit painful. When I say 1 I mean plain fork().
4:57
I think, yeah I'm not sure that everyone agrees with this take by the
5:03
way, but that that it seems to be a fairly influential paper on the topic, and it it certainly gives a lot of
5:08
interesting background if you want to read something about that. So looking at fork() -- I'm pretty sure everyone knows how this
5:14
works but I'm just going to super quickly, go over the the memory map stuff -- so fork() creates a new process
5:20
which is just like the parent. So you call fork() and it returns twice, once in
5:26
the calling process ... that we now call the parent, and then once in the in the child process, and you have
5:34
to check the return value of fork() to find out if you are the child or the the parent. You've essentially been photocopied, and that forking
5:41
operation copies all of the private mappings in the memory map of the parent.
5:47
And I've got an asterisk there because it doesn't really copy it but conceptually it copies the private mappings so that includes the code,
5:54
shared libraries that are mapped in, the executable itself, the global variables that go with all the
6:02
libraries and executable and so on, the call stack that of the calling thread, the heap, everything, all
6:08
the private mappings. The shared mappings, that includes shared memory
6:14
that was set up explicitly using mmap() or maybe the old System V shared memory system calls, those on the other hand
6:21
do not get copied, so I'm showing the parent in orange or yellow whatever that is, and the child in blue and this
6:28
shared memory here, I've got a line connecting them because if the child modifies that memory the parent will see
6:34
the modification and vice versa. It's the same virtual memory object mapped in in two different places whereas the
6:40
private mappings above, the executable and so on, conceptually they're completely separate copies, although
6:46
systems don't actually waste memory on copies, we'll talk about that in a moment. But fork() also has ... to understand
6:56
it and use it safely you have to see how it works, like, how it affects all kinds of other resources or properties of
7:02
of a process. So if you look at the POSIX documentation for fork() I think
7:08
it's got about 26 bullet points and how it interacts with all kinds of different things like signal masks and other
7:14
things like that, so there's a lot of complexity there to actually use it correctly, and you know, we've been tweaking that stuff for years in
7:20
Postgres, and it's really old, because you know ... which things get
7:27
sort of inherited and which things it's bad to inherit and so on, you have to control that stuff in different
7:32
places whereas the alternatives were
7:39
complex in a different way. So like CreateProcess() on Windows I think it has
7:44
about 10 arguments or something and it's got all kinds of flags which you look at, you think well you know, fork() is such a
7:50
beautiful simple thing, why don't we just use that, but the complexity is still there, it's just scattered all over the place and everyone has to learn about it
7:57
and get it wrong lots. So "copy", I put an asterisk on "copy".
8:03
The VAX -- which appeared in David Dewitt's amazing talk just before --
8:10
that machine brought virtual memory concepts as we know
8:17
them to mini-computers. I think all of the ideas were present in way more expensive mainframe
8:24
systems, probably quite quite a long time before that, I don't know too much about those systems, but this was the arrival
8:30
of virtualization of memory in hardware that could, you know, in
8:36
these mini-computer systems that didn't cost millions and millions of dollars
8:41
that... in the 80s
8:46
both main strains of Unix, the BSD Unix systems and the AT&T
8:56
systems, they used VAXes to sort of rebuild the virtual, to build virtual
9:02
memory systems on top and that kind of design, the way that works, influenced
9:08
the even cheaper computers that followed, including the i386 and 68000
9:13
chips with MMUs and so on, so that hardware allowed the kernel to implement
9:19
copy-on-write so that the child process doesn't actually... the earlier PDP
9:25
Unixes would literally copy the entire process memory, they didn't even call it a memory map I don't think I think it
9:32
was just like, here's this chunk of memory, but then those systems probably had something like eight memory pages in total, or something
9:38
like that, so it wasn't too too bad, just because their programs weren't huge.
9:43
But the VAX hardware allowed them to perform copy-on-write, so it became a lot faster, but the page table, which is a
9:52
data structure that has entries for every single page of memory that's in the memory map, that still has
9:57
to be copied in most operating systems. It's not technically necessary,
10:02
they could implement something that shares page tables and you can see that in some operating systems
10:08
like Solaris. Linux huge pages do share the page table.
10:16
I don't understand this stuff well enough to comment on it myself but I have seen some experts saying that it
10:22
doesn't work as well as it could, and there is ongoing work to make regular
10:28
page sizes' page tables be shared on Linux. Then again people have been
10:35
trying to do that for over 20 years; I think there are about four different attempts with patches that
10:40
didn't eventually make it in. The current group of people that are working on that are based at Oracle. I don't know that
10:46
much about that myself. I would assume that Oracle wants that for the same reason that we would want that,
10:52
because they do a lot of forking, and forking when you've got a huge memory map consisting of a very large number of
10:57
pages just has to take time and it also has to occupy a lot of memory. So that's a lot of duplication.
11:05
So what do we do on Windows if we don't have fork(), and yet Postgres is built around fork()?
11:11
It's kind of a miracle of engineering if you ask me that Postgres has been made to work on Windows.
11:18
... There's many things to to look at and think well,
11:25
it seems kind of crazy but it actually works and people are running this in production. It's really
11:33
just a tiny a tiny emulation of the simplest things that you need for
11:38
Postgres. It's certainly not a general emulation of fork(). So I said before that Windows can only create a process with
11:44
the CreateProcess() function, that means that it inherits
11:50
handles, so things like sockets and certain types of things can be inherited
11:56
and you can control that, but the memory map is certainly not, it's just completely replaced. So ...
12:03
it's similar to the previous slide showing showing fork() on on Unix but here I'm showing ... I've tried to capture
12:09
that the executable and the the heap and so on might be at a different address so I've kind of shifted them on the picture if you see what I mean. It's also really
12:15
slow, ... We need the main shared memory segment
12:21
to be in the same place so we try to map it in the same place -- I don't fully understand that code to be honest but it
12:26
seems to have to be prepared to sleep and retry, so I think it, yeah I don't have
12:32
the details in my head right at the moment but basically we arrange for just that one piece of memory to be at the same address which I think
12:39
involves fighting against address space randomization stuff. I know that that scheme adds at least 40 milliseconds to
12:48
a trivial parallel query just because it's so hard to start up a process to simulate fork() in that way.
12:55
Probably it's much worse on a serious size system, I'm not a Windows user so I have never really looked into this, I've
13:01
just studied problem reports and that's the smallest number I've ever seen anyone report for how slow a
13:07
SELECT 1 in a parallel worker, how
13:12
much it adds. So that was about usage number 1 from
13:18
my original 3 different ways to start a a thread of execution. The
13:24
second usage or motivation for creating a thread of execution is to start a different
13:30
program as a subprocess. So back in the old days in early Unix before they had VAX-type virtual
13:37
memory as I said earlier they would literally copy all the memory and and the only way you could create a sub-
13:44
program and run a different a different executable was to fork() first and then have the child call exec() or one of the
13:49
variants of the exec() system call, so you would copy all of that memory and then throw it away and load in a new
13:57
executable. I'm showing that ... all of these all of these blue things in the middle column of boxes get copied
14:03
and then thrown away and then obviously that was a bit silly. So the BSD guys came up with
14:12
something that they described as a kludge. At this time they were working -- I
14:17
think I have this right -- at this time they were working on getting VAX-style virtual memory to work but they
14:25
still had the literal copying going on. There's some there's quite an
14:31
interesting story about how they invented mmap() and so on, but
14:36
there was some problem, some strange legal problems that led to them ... having to change
14:43
implementation around so that was a little bit delayed, but they also wanted to be able to start subprograms
14:50
really quickly, because, if you think of for example a shell script, a shell needs to do an awful lot of that,
14:55
every single line in a shell script is probably going to create a subprogram. So vfork() was invented, and it's a
15:03
very strange thing, it's very easy to shoot yourself in the foot with it, and generally normal code should never be
15:08
calling it. It creates a subprocess, suspends the parent process, the child process still has the parent's memory map
15:16
so if it changes anything including a variable on the stack or a ... global variable or something
15:22
in the heap then the parent will see that change, which is very strange.
15:28
It's almost like you're a thread, but you're still a process. So it's very
15:34
easy to completely corrupt both processes, and that's why it suspends the
15:40
parent until the child either exec()s or exit()s, so either it stops running because
15:46
something fails or it loads a totally different program into memory. At that
15:51
moment the parent is allowed to continue. That is basically a hack, it
15:57
allows sub-programs to be started very quickly. You can see that it's sort of a it's a variation on what on how fork()
16:03
works, and it's it's kind of an admission that the original concept of making fork() super simple and not even having...
16:09
fork has no arguments. So people finish up having to make
16:14
weird variations of it, because it just they had special requirements including going fast in this common case.
16:22
That was actually in POSIX but then they removed it and and standardized posix_spawn(). I don't see much software using
16:30
posix_spawn, but then again most people don't have to call vfork() either because people
16:35
use things like system() and popen() from libc, and you will find that they still do that inside. So POSIX has removed vfork(),
16:42
but you'll see that most systems still use it internally because it's very useful. They might use it to implement
16:49
posix_spawn() for example. And the third thing is creating a threads. With POSIX -- I'm
16:57
showing pthread_create() because that's the POSIX function. Windows CreateThread() is essentially the same.
17:05
There's no separate process, there's no separate memory map, you're sharing almost
17:11
everything. You just have a new thread of execution. Context switches between
17:16
those threads may be more efficient due to the way that virtual memory
17:22
hardware works. Not having to be reprogrammed, shootdown, TLB shootdowns
17:28
and all that kind of stuff. There's no extra overhead for copies of the page
17:33
table of course because it's the same page table. But because you're
17:38
sharing global variables and file descriptors, of course you have a whole lot of new problems in your life. You have to make sure that they don't
17:44
trample each other, and you've got all kinds of new interlocking problems. So in 2011 the C standard added
17:55
a new standardized interface to the C library called threads.h. I think
18:03
we can't use it yet, because Apple forgot to implement it -- I believe that they're working on it, I've seen
18:09
some clues about that. On Windows it's been added to Visual Studio
18:14
2022, which seem promising but MinGW doesn't have that
18:20
yet and I don't know what to do about that and I'm not really a Windows guy so I'm not sure where to find out if that...
18:27
hopefully that should eventually work but I don't know when it's going to happen. One thing that I found while
18:33
working on this stuff and trying to see if we could use C11 threads for PostgresL: it doesn't have some things
18:41
that are quite useful in pthreads, and one thing that jumped out at me was
18:46
that it doesn't have static initializers, so when you create a mutex you can't just give it a special value, you have to
18:52
do a whole dance and initialize it with a function call and so on which is probably not a
18:57
problem for new code but it's slightly annoying that you can't just sort of drop it into existing code that's
19:04
written with Pthreads, because we use those initializers all over the place, and it it's quite hard to find a place
19:10
to initialize those things. So I found that a bit odd. I'll
19:15
just mentioned in passing, as it's not really directly related -- well it is very related to threads, but it's not directly
19:21
related and you don't need to do this to make Postgres multi-threaded -- but at the same time C11 also introduced a
19:27
standard set of atomics operations. Postgres has a bunch of atomic stuff
19:32
which is very similar looking and I think that's because the C11 atomic
19:38
stuff was in development and drafts had been seen. I think I think it was mostly Andres who did that work and I
19:44
think he was influenced and trying to make it look roughly the same, and therefore it's not that
19:51
surprising that I was able to completely replace everything with
19:57
C11 atomics. With a few little quirks it it works fine on all my operating systems that I use regularly, at
20:04
least three or four different systems. When we're ready to use C11 -- I don't
20:12
have an opinion on that, there's going to be a very good talk on that shortly -- we might decide to do that, but to do
20:20
that you'd really have to determine that all of the handrolled assembly and
20:26
other magic that we've accumulated for doing that, you'd have to determine that it's not going
20:32
to cause any un unexpected regressions, and unfortunately that involves poking around on a load of different
20:38
architectures and operating systems and so on, and compilers. That's essentially orthogonal so I just
20:45
mentioned it in passing because it kind of goes with the thread stuff that came in C11. All right, I've worked on a few
20:53
different projects in Postres that made me want threads a lot, and in my earlier jobs and projects I worked on
21:00
threads were just taken for granted. ...
21:06
When I started working on Postgres full-time it took some adjustment, and
21:12
and for many years I sort of forgot about them and you know you start forgetting the arcana of working with
21:17
them as well. But a couple of different projects I worked on led me to think about them a lot and start
21:23
trying to find out what problems you need to solve to use them. You don't have to read this wall of text, but
21:29
this is just something I wrote on my blog in 2018 when I'd been working on parallel hash joins. To make that work
21:36
we had to come up with some way to share dynamic memory -- memory that wasn't
21:42
originally allocated by the postmaster and that involved so
21:47
much machinery that I'm really not that happy with it and I know that most people who try to use the DSA
21:53
system probably curse my name. It's quite difficult to use probably but that's because it's trying to do something really tricky. It's essentially a kind of
21:59
a fake software virtual memory system -- well not quite -- but it's it's doing address decoding and it has overheads
22:06
and it's fairly tricky, so even when I proposed that stuff
22:13
I was already saying "we've got to get rid of this, I know we haven't committed it yet but we've got to get rid of it!" and the way to get rid of it
22:20
of course is to share the memory map. There's probably some other ways you
22:25
could get rid of it but they would also be complicated, and we discussed that at the time when that was being worked on.
22:30
"get rid of it at the same time?" Yeah that's what I was thinking...
22:36
this thing we got to get rid of it yeah... More generally, the people that set out to work on
22:43
adding parallel query execution to Postgres, Robert Haas and others,
22:50
they made the choice not to try and make Postgres multi-threaded first.
22:56
I can't speak for their motivations but my understanding is that approximately that it's really hard and
23:04
could sink the whole project, and they just wanted parallel query execution and they could see a pathway. That
23:09
makes a lot of sense, and there certainly are other ... relational database systems that manage to do that.
23:16
um Yeah I think it's extremely difficult for them as well, that would be my guess.
23:22
I'll talk about why in a moment. Another project I've done a lot of work on, I've been
23:29
working on small niche parts of the AIO system, which is Andres Freund's project,
23:35
which has... a large piece of that, the real
23:41
subsystem itself, is shipping in Postgres 18. Well I don't want to jinx that: it's in Postgres 18 beta 1.
23:49
That's a really massive and complicated project, and it adds true asynchronous I/O to to Postgres.
23:56
A lot more work needs to be done. At the moment it has background processes that can run your I/O operations for you, or it
24:02
has io_uring as a special mode that works on Linux, but you can do native AIO on
24:10
lots of other operating systems, for example Windows. Those systems are very mature and they're used
24:16
by other databases on those operating systems that support that. I've written patches that kind of work okay
24:23
on Windows -- programming on Windows is very hard for me because I just send patches to CI and see if the tests pass,
24:28
usually they don't and then you start again. The first port I worked on
24:34
was getting it working on FreeBSD, which is my preferred hacking operating system.
24:40
[Music] The relevance to threads here is that the designers of these systems didn't
24:48
imagine for even a second that you would be using a multi-process design. It just clearly didn't even enter their
24:54
minds... they don't even tell you in the documentation that it doesn't work, it's so obvious I guess, like, who would do
25:01
that, right? What I'm describing is: one process starts an I/O, starts reading a chunk of data off disk into
25:08
memory and then it goes away and does something else -- I don't know, maybe it's waiting for a lock
25:13
somewhere. Some other process comes along and wants to access a buffer, and that buffer is associated with that I/O that's
25:20
running and in fact it's already completed. It can't wait for that first process to consume the
25:27
completion event from the kernel and terminate the I/O and update all the buffer headers and so on and make the
25:33
buffer BM_VALID because you might deadlock.
25:39
Just the way the levels of locking and so on work here ... asynchronous I/O
25:44
generally, to be deadlock free and even just to be performant, it needs to ...
25:50
be able to complete any I/O that it can get its hands on from any back end. You can't do that directly
25:57
using the Windows AIO interfaces and or the POSIX
26:02
ones that are used on FreeBSD and a bunch of other operating systems. So I did manage to make it work... it was
26:09
really not pretty and it really made me think ... you know ... should I propose these patches, or should I just go and work on
26:15
threading first? Yeah I still still don't know the answer to that question. I'll write about that on
26:22
the mailing list soon. Alright, there's a whole lot of other places where you can see that operating system designers
26:29
didn't even contemplate that you would want to do something cross-process: for example, Windows has futexes but they
26:34
don't work cross-process. There's a bunch of IPC-like things, semaphores
26:41
and so on, that don't work cross-process on a number of operating systems. macOS
26:47
is an example that I happen to have right in front of me ... this computer ... it would be really easy
26:54
for them to make that work, I know how you do that, it's very easy, it's just a matter of some
27:00
address mapping stuff that's not even complicated, but they didn't do it, which just shows that no one wants it,
27:06
which is really interesting to me, that that you know, we're struggling with things that other people like just don't
27:14
do. I thought that was kind of interesting. In theory Postgres could
27:19
use even with a multi-process model that we have today ... I've hacked I've hacked up prototypes of this before and it works
27:25
fine ... you can delete half of our LWLock stuff and replace it with Pthread mutexes
27:30
for example, even though you're using processes, and you should get pretty good performance, you would think, I don't know
27:35
that maybe, maybe not, but we have a linked list of semaphores that we go and
27:40
poke so ... if you get the kernel to manage the wait lists probably it can
27:46
do a better job, I don't know but we couldn't consider that because it doesn't work cross-process on some
27:52
systems and POSIX allows for that. It's probably closely related... these things are all kind of related.
28:01
So, stepping right back and moving to the second part of the talk, let's talk about how um you would arrange the
28:07
threads of execution in general in a database. For this, I wanted to get
28:13
some terminology and get some kind of overview of what other databases are doing. This is actually a fairly old
28:18
paper, 2007 ... look that name Stonebraker again ... anyway
28:25
so it's pretty old and the things that it says about other databases are probably out of date by now, but they
28:31
still give some idea, and the things they say about us are not out of date. The process model that we use today is
28:38
fairly straightforward: for each socket coming into Postgres
28:43
there's a process running, and then that process manages the state for a session.
28:48
There could be many of those, you might have a thousand sockets, ten thousand. Then each one's got a process, each one's
28:53
got a session and the operating system does all CPU multiplexing. Maybe you've got a two-core system -- let's
29:00
pretend it's a very small computer -- and it's up to the operating system to figure out how to run your 10,000
29:06
backends if they are all runnable right now. So there's a whole lot of context switching between
29:13
those processes. Here's a little quote from that paper -- I just, like a
29:18
scrapbook, cut out the little descriptions from that
29:24
paper -- this is or at least was in 2007 Oracle's default process model. We got a
29:30
name drop here: "PostgreSQL runs the process-per-DBMS-worker model exclusively on all supported operating systems".
29:40
So... the simplest change you could make, I think, and I think the most
29:46
realistic design we could consider, is to keep everything the same as now to the
29:52
maximum extent possible and just use threads. And
29:58
I'll say what I'm excluding on a later slide.
30:03
The idea is to try and make everything just the same shape ...
30:08
and then find all the things that break and fix those. So you've ... still got one thread for each socket
30:14
coming in, and that's associated with the session. You're still letting the operating system map all those threads
30:22
onto only two hardware cores, so it does all the context switching between them. Context switching between those threads
30:27
is probably cheaper than context switching between processes. It says here that DB2 defaulted to
30:34
that model, in 2007 at least. MySQL uses that model. It's fairly
30:40
straightforward and I think it's probably a lot more efficient than what we're doing now. Many people have
30:46
tried this. I've become aware of a lot of different
30:51
prototypes and people who've written about trying trying to get this working. I think it's kind of the most
30:57
obvious path forwards. There's another idea, just for completeness: in the paper it talks
31:03
about another idea which I have never even considered. It's kind of madness but in
31:09
some systems they had ways of doing multiplexing between... you could
31:16
call them green threads. This idea is basically dead in C. There are a bunch of other languages that do this
31:23
with great results... they might have these kind of green threads or whatever you want to call them. Windows
31:31
has fibers. I think the idea has basically gone out of fashion in Windows as well.
31:37
For native code I should say, but some other languages do this stuff. It
31:44
just doesn't make that much sense for C and in particular the stuff you need to do that ... POSIX used to have these calls
31:50
getcontext() and setcontext(). You could basically photocopy your stack and all your registers, and you need to be able to do that so you can switch between
31:56
threads of execution. Or you could also do it in a whole lot of horrible non-portable ways
32:02
that are very architecture- and operating system-dependent. We certainly wouldn't want to do that. So that whole concept is is not really doable, so I'm
32:09
just mentioning that for completeness, since the paper calls that one of the obvious ideas. There's a kind of
32:16
Holy Grail ... it's my understanding that this is approximately the state-of-the-art for this type of thing.
32:22
This involves about four different boil-the-ocean projects coming together and I'm not even seriously
32:28
considering it, right, but the ideal system would have one
32:35
thread running per hardware core, keep them busy as much as possible, you've got your own scheduler which has got work
32:41
to do and ... your I/O is is asynchronous so it's driven by events coming in and if a message
32:47
comes in on the socket is actually the completion of a recv() that you started earlier and queues up a little work task. But
32:53
first you need to take your whole executor and smash it into pieces and be able to run those small pieces on your
33:00
scheduler, so that that's just a whole... you absolutely cannot start with that in
33:05
mind. But it might ... the reason I mention it though, is that, well, firstly it says here that a number of
33:11
other systems do use that design: SQL Server defaults to that model, almost
33:17
everyone uses that model, it's probably the most efficient model, I think
33:22
Sybase does the same, probably many other systems do the same and certainly a lot of HPC systems that aren't databases in
33:28
general use this type of approach. And runtimes for other languages. So the reason I
33:35
mentioned that is because one of the most basic and obvious problems when Postgres becomes
33:41
multi-threaded is that it's totally loaded up with global variables -- just millions of them! -- and they represent
33:49
current transaction state, current session state, they represent GUCs and um all sorts of transient
33:57
things. I'm aware of more than
34:06
half a dozen people who have tried to just turn them all into thread-
34:11
local variables, which basically works right, it makes them behave as though... ... with the same syntax ...
34:17
it basically means that you continue to treat everything basically the same way.
34:22
And what Heikki has been doing in his branch, which he's shared publicly and
34:28
which he talked about at this conference -- I actually can't remember,
34:33
oh no sorry in the European conference -- he talked about his
34:39
branch which is public and I'm referring to a few things in his branch here. Instead of just going and directly
34:46
marking everything thread-local he's got a classification scheme where he goes through and annotates what different variables are for, which I think is a
34:52
very good idea, and really they just turn into thread-locals but in future there could be some different schemes. Another
34:58
idea would be to take all that stuff and stuff it into objects and then pass pointers to
35:04
those objects around. And you would then need to add a lot of extra arguments to a lot of different functions. I think
35:10
it would be really complicated, but it is probably what you need to do to get to that Holy Grail
35:16
design. And not just session but also transaction and some other objects. Actually this is code that's
35:22
actually in master, because I thought about that a lot when I was doing some parallel query work and I was like...
35:29
essentially, "hey guys we should have a session sturct, and here's one thing I need to solve my current problem", and
35:36
I was kind of imagining that we might start to put other things in there. But I came to understand
35:42
that it's probably not so easy, and it's probably better not to do to make
35:47
unnecessary changes, and try to make the patch set digestible, and I
35:52
think it's a reasonable place to start just to use thread-locals everywhere. Later on we might eventually come
35:58
around to something like that. Okay, so here's a list of people I know of
36:04
who've tried to do thread local based things with with various success. The CMU Peloton project, they also
36:11
changed it all to C++, pretty impressive. Postgress Pro had a working prototype that they proposed to the list and no
36:17
one was really that interested at the time, which is kind of a shame. There were multiple early attempts to
36:24
port Postgres to Windows using threads and they were able to run to some degree. I found a guy on Reddit who spent
36:33
some time getting Postgres to work like this and then talked about it on Reddit
36:38
and then read our Wiki, which said we don't want this in our "features we don't want". That's been deleted now but that was a
36:44
thing we had on the "features we don't want". So yeah the reason that it was on the features ... one thing is that it's been discussed many times, people
36:51
have tried this many times (including myself). Heikki's got a pretty cool
36:56
branch that I think yeah. There's a number of people now who really do want this and I think there's a
37:04
chance it could really happen this time around. Fingers crossed. Another little
37:09
architectural choice you might want to think about is whether... in a multi-process mode you've got
37:16
the postmaster and all the child processes how does that look, do you still need a postmaster? I think you do,
37:22
you certainly do if you're going to have a multi-process and a multi-thread mode with a switch -- which I think we should do --
37:28
because otherwise too many things will break in the first version. Many people have said, "well why can't you just use SystemD or
37:35
something like?" Well one problem is that of the 11 operating systems we target almost all of them are not
37:44
Linux. Also not even all Linux systems use SystemD, and I think there
37:49
might be some extra reasons to do with state management. The fact that we probably want to have
37:55
both multi-processes and multi-thread mode in the hypothetical first version of this means that you probably
38:02
just want to have a postmaster anyway. I don't know maybe it should be renamed to "supervisor" because it wouldn't do much
38:07
anymore, it would mostly just be restarting the thing if it crashes, because all of the logic to ...
38:13
start new threads and so on would just be in the main container process,
38:19
but it still needs to exist because if that process crashes it needs to coordinate restarting it and letting
38:25
recovery run. I'm going to talk about some work in progress, actual concrete stuff.
38:31
So you can look at Heikki's branch. It's got some code which has come from various places, getting rebased from
38:38
master of course, and I think Stas Kelvich seems to be helping him with some of that stuff. A number of pieces
38:45
that you need to make that work have just been done independently in master and are still being
38:52
discussed and it will be in the next commitfest. They flow into that
38:57
branch so you can try it out and run it and probably crash pretty
39:02
soon, but you can run some queries and you can get some stuff working and you can see the basic idea and maybe even
39:09
help. So yeah, general goals...
39:15
we almost certainly want to run both multi-process and multi-threaded modes,
39:21
especially because there's a whole ecosystem of extensions
39:28
and you know it'll take a long time for all the kinks to be ironed out and people to understand the new ... and
39:34
and also just generally for the whole thing to be stabilized and debugged ... and for all of the performance
39:40
implications to be fully understood and so on. So we want a switch that makes it exactly like it is
39:47
now. The flip side there's a whole bunch of non-goals for the
39:53
prototype work. So each back end is a thread
39:59
now and so they they're in a process and they could see each other's file descriptors. The file descriptors they
40:05
opened, you know this just one set of file descriptors for a process, but trying to share them between backends,
40:11
that's a whole project that you don't need to deal with to make this work. And as we all know, ... the more
40:19
crazy projects you tie together with string, the more likely that the whole thing collapses, and doesn't work. It's
40:24
the same with the memory used by RelCache .. and various other things. One of the famous ways to make
40:31
your system run out of memory is to have many backends with a lot of cached data. We don't really do cache invalidation [correction: replacement]
40:36
for most of these caches and it's duplicated in every backend, so many people have wanted to share that
40:43
stuff but cache invalidation is one of the famously hard things, and you
40:48
know that's all tied up with... it's complicated, again let's not try and do that, let's just let each back end waste
40:54
a whole lot of memory by duplicating that stuff, so that multi-thread mode and multi-process mode are almost exactly
41:00
the same in their behavior. There's a whole range of possibilities with MemoryContexts... again,
41:07
keep it simple, make them work as if they were processes, don't
41:14
share any MemoryContext between between backends. The whole DSM/DSA thing, although I would like to see it go,
41:21
it's better to just leave it all as it is and then later on that could eventually ... maybe you can't
41:27
even do that until the multi-process mode eventually goes away, which would have to be the goal, right? I think the
41:33
idea of having both multi-process and multi-threaded is to allow a multi-year, you know, transition for the
41:40
ecosystem. And some other non goals
41:46
there which are I guess self-explanatory. So some concrete work,
41:52
in-progress stuff: how should we even talk to threads? How should we talk to the operating system? My current
41:59
idea is that it's a little too soon to use C11, mainly because of macOS,
42:06
but I don't want to make up new names for things, I like the C11 names, I like to imagine that in the future eventually
42:12
this runs everywhere and we do just use C11. So I proposed pg_threads.h to be
42:19
like from the C standard but with pg_ prefixes everywhere.
42:25
I did have a patch in the last cycle which had a few problems, I've got
42:30
a better version that I'll propose again soon. It actually works out to be a net-
42:36
zero patch, ... it deletes as many lines as it adds because we have many
42:42
little things like that all over the place and some of them have bugs and they're inconsistent and so on, so you know, it's a kind of a code
42:48
cleanup thing as well. But people don't have to agree with the my idea of using the C11 names, I
42:56
don't know, some people prefer to use the POSIX names and then try and make Windows look like POSIX. I don't like that because
43:02
it never really will work exactly like POSIX so why even pretend, why not just make a new set of names that work in a
43:09
certain way, and we define what the set of things we want to use is and we can deal with the differences.
43:16
That's kind of my approach. Over the last... certainly in this
43:22
release but probably... (yeah almost out of time here) there's been a whole lot
43:28
of work done by many different people to remove all kinds of non-multi-thread- safe code all over the place, and that's
43:33
really cool, it's great to see that happening. ... There's there's plenty of space for anyone
43:39
who's interested in this stuff to go looking for those kinds of things, particularly to do with locales we kicked out a lot of global locale usage, it's not
43:46
finished yet, there's a whole lot of places where we have static buffers and so on and and yeah so that's
43:51
little pieces that can be done just in master without having to get involved in the in in Heikki's public
43:58
branch. A major piece of work here and which was proposed last cycle -- I think it became a bit too complicated and we kind
44:05
of ran out of time -- is to chase down all the places where Postgres uses signals and get rid of them, because they
44:12
assume ... and I mean signals to communicate with a backend... obviously
44:17
you can't use signals to communicate with the backend if the backend is no longer a process.
44:26
Maybe you could use pthread_kill() but yeah it doesn't really seem to make much
44:32
sense, and in fact while studying that it became very clear to me that our
44:37
system has become, over the years, a cooperative system and therefore
44:43
latches should probably be be the basis for almost all wakeups in the system. That technology works pretty well. I
44:51
proposed that, and then Heikki, who was the inventor of the latch system, he said "why don't we also get rid of
44:56
latches?", and he did some further refactoring work and so the current
45:01
proposal creates this new thing called an "interrupt" and and gives it a meaning and makes it into backend... and
45:07
so on and we figured out how to do that in a way that lets us get rid of a whole lot of use of signals. There's some
45:14
stuff on the wiki, I plan to try and update it some more and try and keep it
45:19
up-to-date with what's happening, but you know I tried to survey stuff like "where are all the places we fire signals
45:25
all over the place? how do we kill that one?", you know and the answer is generally that somehow it's got to be latches, or rather interrupts.
45:32
That work is ongoing, there'll be new versions of that patch set in this
45:39
cycle. That's the end of my talk. I just wanted to say that it's really a complicated, big project
45:45
and you know it's going to take a lot of people a lot of effort to try and get something that is is stable and
45:52
sustainable and works for the ecosystem, but I think we have to do it I don't think we can go on running as a
45:58
multi-process system. That's the end of my talk, and I think there might even be a few seconds for questions.
