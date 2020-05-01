sign-off
========
**Certify your work by adding your "Signed-off-by: " line**

[This text is borrowed from the Git project.](https://git.kernel.org/pub/scm/git/git.git/tree/Documentation/SubmittingPatches?id=HEAD)

To improve tracking of who did what, we've borrowed the
"sign-off" procedure from the Linux kernel project on patches
that are being emailed around.  Although ZooDB is a lot
smaller project it is a good discipline to follow it.

The sign-off is a simple line at the end of the explanation for
the patch, which certifies that you wrote it or otherwise have
the right to pass it on as a open-source patch.  The rules are
pretty simple: if you can certify the below D-C-O:

dco
===
** Developer's Certificate of Origin 1.1 **
[(original)](https://developercertificate.org/)
____
```
By making a contribution to this project, I certify that:

a. The contribution was created in whole or in part by me and I
   have the right to submit it under the open source license
   indicated in the file; or

b. The contribution is based upon previous work that, to the best
   of my knowledge, is covered under an appropriate open source
   license and I have the right under that license to submit that
   work with modifications, whether created in whole or in part
   by me, under the same open source license (unless I am
   permitted to submit under a different license), as indicated
   in the file; or

c. The contribution was provided directly to me by some other
   person who certified (a), (b) or (c) and I have not modified
   it.

d. I understand and agree that this project and the contribution
   are public and that a record of the contribution (including all
   personal information I submit with it, including my sign-off) is
   maintained indefinitely and may be redistributed consistent with
   this project or the open source license(s) involved.
```
____

then you just add a line saying

```
	Signed-off-by: Random J Developer <random@developer.example.org>
```

This line can be automatically added by Git if you run the git-commit
command with the -s option.

Notice that you can place your own Signed-off-by: line when
forwarding somebody else's patch with the above rules for
D-C-O.  Indeed you are encouraged to do so.  Do not forget to
place an in-body `From: ` line at the beginning to properly attribute
the change to its true author (see (2) above).

real-name
=========
Also notice that a real name is used in the Signed-off-by: line. Please
don't hide your real name.
